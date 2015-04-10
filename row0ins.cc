#include "row0ins.h"
#include "dict0dict.h"
#include "dict0boot.h"
#include "trx0undo.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "mach0data.h"
#include "que0que.h"
#include "row0upd.h"
#include "row0sel.h"
#include "row0row.h"
#include "rem0cmp.h"
#include "lock0lock.h"
#include "log0log.h"
#include "eval0eval.h"
#include "data0data.h"
#include "usr0sess.h"

#define ROW_INS_PREV		1
#define ROW_INS_NEXT		2

/*创建一个insert node任务对象*/
ins_node_t* ins_node_create(ulint ins_type, dict_table_t* table, mem_heap_t* heap)
{
	ins_node_t*	node;

	node = mem_heap_alloc(heap, sizeof(ins_node_t));
	node->common.type = QUE_NODE_INSERT;
	node->ins_type = ins_type;

	node->state = INS_NODE_SET_IX_LOCK;
	node->table = table;
	node->index = NULL;
	node->entry = NULL;
	node->select = NULL;

	node->trx_id = ut_dulint_zero;
	node->entry_sys_heap = mem_heap_create(128);
	node->magic_n = INS_NODE_MAGIC_N;	

	return node;
}

/*根据node要插入的table索引，构建各个索引所需要的记录值，并将这些记录值放入node->entry_list当中*/
static void ins_node_create_entry_list(ins_node_t* node)
{
	dict_index_t*	index;
	dtuple_t*		entry;

	ut_ad(node->entry_sys_heap);

	UT_LIST_INIT(node->entry_list);

	index = dict_table_get_first_index(node->table);
	while(index != NULL){
		entry = row_build_index_entry(node->row, index, node->entry_sys_heap);
		UT_LIST_ADD_LAST(tuple_list, node->entry_list, entry);

		index = dict_table_get_next_index(index);
	}
}

/*将系统列（row id/trx id/roll ptr）加入到要插入的记录行（node->row）中*/
static void row_ins_alloc_sys_fields(ins_node_t* node)
{
	dtuple_t*	row;
	dict_table_t*	table;
	mem_heap_t*	heap;
	dict_col_t*	col;
	dfield_t*	dfield;
	ulint		len;
	byte*		ptr;

	row = node->row;
	table = node->table;
	heap = node->entry_sys_heap;

	ut_ad(row && table && heap);
	ut_ad(dtuple_get_n_fields(row) == dict_table_get_n_cols(table));
	/*row id*/
	col = dict_table_get_sys_col(table, DATA_ROW_ID);
	dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
	ptr = mem_heap_alloc(heap, DATA_ROW_ID_LEN);
	dfield_set_data(dfield, ptr, DATA_ROW_ID_LEN);
	node->row_id_buf = ptr;

	/*mix id*/
	if(table->type == DICT_TABLE_CLUSTER_MEMBER){
		col = dict_table_get_sys_col(table, DATA_MIX_ID);
		dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
		len = mach_dulint_get_compressed_size(table->mix_id);
		ptr = mem_heap_alloc(heap, DATA_MIX_ID_LEN);
		mach_dulint_write_compressed(ptr, table->mix_id);
		dfield_set_data(dfield, ptr, len);
	}

	/*trx id*/
	col = dict_table_get_sys_col(table, DATA_TRX_ID);
	dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
	ptr = mem_heap_alloc(heap, DATA_TRX_ID_LEN);
	dfield_set_data(dfield, ptr, DATA_TRX_ID_LEN);
	node->trx_id_buf = ptr;

	/*roll ptr*/
	col = dict_table_get_sys_col(table, DATA_ROLL_PTR);
	dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
	ptr = mem_heap_alloc(heap, DATA_ROLL_PTR_LEN);
	dfield_set_data(dfield, ptr, DATA_ROLL_PTR_LEN);
}

/*将row作为INS_DIRECT方式插入，并设置到node执行任务中,相当于用row初始化node*/
void ins_node_set_new_row(ins_node_t* node, dtuple_t* row)
{
	node->state = INS_NODE_SET_IX_LOCK;
	node->index = NULL;
	node->entry = NULL;

	node->row = row;
	mem_heap_empty(node->entry_sys_heap);
	/*构建各个索引所需的记录*/
	ins_node_create_entry_list(node);
	/*设置row的系统列*/
	row_ins_alloc_sys_fields(node);

	node->trx_id = ut_dulint_zero;
}

/*修改辅助索引上的索引记录,是插入操作修改在辅助索引上对应的del marks记录，这种情况是保护多版本一致性读而做的*/
static ulint row_ins_sec_index_entry_by_modify(btr_cur_t* cursor, dtuple_t* entry, que_thr_t* thr, mtr_t* mtr)
{
	mem_heap_t*	heap;
	upd_t*		update;
	rec_t*		rec;
	ulint		err;

	rec = btr_cur_get_rec(cursor);

	ut_ad((cursor->index->type & DICT_CLUSTERED) == 0);
	ut_ad(rec_get_deleted_flag(rec));/*记录被设置了del mark*/

	heap = mem_heap_create(1024);
	/*比较entry和rec的不同，将不同的列放入updage中*/
	update = row_upd_build_sec_rec_difference_binary(cursor->index, entry, rec, heap); 
	/*通过辅助索引修改对应的索引记录*/
	err = btr_cur_update_sec_rec_in_place(cursor, update, thr, mtr);

	mem_heap_free(heap);
}

/*修改聚集索引上的索引记录,是插入操作修改在聚集索引上对应的del marks记录，这种情况是保护多版本一致性读而做的*/
static ulint row_ins_clust_index_entry_by_modify(ulint mode, btr_cur_t* cursor, big_rec_t** big_rec, dtuple_t* entry, ulint* ext_vec,
					ulint n_ext_vec, que_thr_t* thr, mtr_t* mtr)
{
	mem_heap_t*	heap;
	rec_t*		rec;
	upd_t*		update;
	ulint		err;

	ut_ad(cursor->index->type & DICT_CLUSTERED);

	*big_rec = NULL;
	rec = btr_cur_get_rec(cursor);
	ut_ad(rec_get_deleted_flag(rec)); /*记录被设置了del mark*/

	heap = mem_heap_create(1024);
	/*找到不同的*/
	update = row_upd_build_difference_binary(cursor->index, entry, ext_vec, n_ext_vec, rec, heap);
	if(mode == BTR_MODIFY_LEAF){
		/*乐观式修改记录*/
		err = btr_cur_optimistic_update(0, cursor, update, 0, thr, mtr);
		if(err == DB_OVERFLOW || err == DB_UNDERFLOW)
			err = DB_FAIL;
	}
	else{
		ut_a(mode == BTR_MODIFY_TREE);
		err = btr_cur_pessimistic_update(0, cursor, big_rec, update, 0, thr, mtr);
	}

	mem_heap_free(heap);
}

/*插入记录时检查唯一键的唯一性*/
static ibool row_ins_dupl_error_with_rec(rec_t* rec, dtuple_t* entry, dict_index_t* index)
{
	ulint	matched_fields;
	ulint	matched_bytes;
	ulint	n_unique;
	ulint   i;

	n_unique = dict_index_get_n_unique(index);

	matched_fields = 0;
	matched_bytes = 0;
	/*比较entry和rec的相同内容的列*/
	cmp_dtuple_rec_with_match(entry, rec, &matched_fields, &matched_bytes);
	if(matched_fields < n_unique) /*已经有列不一样了*/
		return FALSE;

	/*如果索引是辅助索引，那么列值在SQL NULL类型下是可以相同的*/
	if (!(index->type & DICT_CLUSTERED)) {
		for (i = 0; i < n_unique; i++) {
			if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i)))
				return FALSE;
		}
	}

	/*如果rec是被删除的，那么可以认为是不重复的*/
	if(!rec_get_deleted_flag(rec))
		return TRUE;

	return FALSE;
}

/*外键约束，如果上层约束行被删除或者修改，那么下层被约束行对应的记录要么被删除，要么置为NULL*/
static ulint row_ins_foreign_delete_or_set_null(que_thr_t* thr, dict_foreign_t* foreign, btr_pcur_t* pcur, mtr_t* mtr)
{
	upd_node_t*	node;
	upd_node_t*	cascade;
	dict_table_t*	table = foreign->foreign_table;
	dict_index_t*	index;
	dict_index_t*	clust_index;
	dtuple_t*	ref;
	mem_heap_t*	tmp_heap;
	rec_t*		rec;
	rec_t*		clust_rec;
	upd_t*		update;
	ulint		err;
	ulint		i;
	char		err_buf[1000];

	ut_a(thr && foreign && pcur && mtr);

	node = thr->run_node;
	ut_a(que_node_get_type(node) == QUE_NODE_UPDATE);
	if(!node->is_delete)
		return DB_ROW_IS_REFERENCED;

	if(node->cascade_node == NULL){
		node->cascade_heap = mem_heap_create(128);
		node->cascade_node = row_create_update_node_for_mysql(table, node->cascade_heap);
		que_node_set_parent(node->cascade_node, node);
	}

	cascade = node->cascade_node;
	cascade->table = table;

	if (foreign->type == DICT_FOREIGN_ON_DELETE_CASCADE)
		cascade->is_delete = TRUE;
	else{
		cascade->is_delete = FALSE;
		if (foreign->n_fields > cascade->update_n_fields) {
			/* We have to make the update vector longer */
			cascade->update = upd_create(foreign->n_fields, node->cascade_heap);
			cascade->update_n_fields = foreign->n_fields;
		}
	}

	index = btr_pcur_get_btr_cur(pcur)->index;
	rec = btr_pcur_get_rec(pcur);
	if(index->type & DICT_CLUSTERED){
		clust_index = index;
		clust_rec = rec;
	}
	else{
		/* We have to look for the record in the clustered index in the child table */
		clust_index = dict_table_get_first_index(table);

		tmp_heap = mem_heap_create(256);
		ref = row_build_row_ref(ROW_COPY_POINTERS, index, rec, tmp_heap);
		btr_pcur_open_with_no_init(clust_index, ref, PAGE_CUR_LE, BTR_SEARCH_LEAF, cascade->pcur, 0, mtr);
		mem_heap_free(tmp_heap);

		clust_rec = btr_pcur_get_rec(cascade->pcur);
	}

	if (!page_rec_is_user_rec(clust_rec)) {
		fprintf(stderr, "InnoDB: error in cascade of a foreign key op\n"
			"InnoDB: index %s table %s\n", index->name, index->table->name);

		rec_sprintf(err_buf, 900, rec);
		fprintf(stderr, "InnoDB: record %s\n", err_buf);

		rec_sprintf(err_buf, 900, clust_rec);
		fprintf(stderr, "InnoDB: clustered record %s\n", err_buf);

		fprintf(stderr,"InnoDB: Make a detailed bug report and send it\n");
		fprintf(stderr, "InnoDB: to mysql@lists.mysql.com\n");

		err = DB_SUCCESS;

		goto nonstandard_exit_func;
	}

	/* Set an X-lock on the row to delete or update in the child table */
	err = lock_table(0, table, LOCK_IX, thr);
	if(err == DB_SUCCESS) /*一个LOCK_IX获得成功，表示这时候没有S-LOCK和X-LOCK,可以尝试获取的一个X-LOCK行锁*/
		err = lock_clust_rec_read_check_and_lock(0, clust_rec, clust_index, LOCK_X, thr);
	/*无法获得事务锁，直接失败？？*/
	if(err != DB_SUCCESS)
		goto nonstandard_exit_func;

	/*记录被删除了*/
	if(rec_get_deleted_flag(clust_rec)){
		err = DB_SUCCESS;
		goto nonstandard_exit_func;
	}

	/*将被约束表中的对应记录行的列值设置为SQL NULL*/
	if (foreign->type == DICT_FOREIGN_ON_DELETE_SET_NULL) {
		update = cascade->update;
		update->info_bits = 0;
		update->n_fields = foreign->n_fields;

		for (i = 0; i < foreign->n_fields; i++) {
			(update->fields + i)->field_no = dict_table_get_nth_col_pos(table, dict_index_get_nth_col_no(index, i));
			(update->fields + i)->exp = NULL;
			(update->fields + i)->new_val.len = UNIV_SQL_NULL;
			(update->fields + i)->new_val.data = NULL;
			(update->fields + i)->extern_storage = FALSE;
		}
	}

	btr_pcur_store_position(pcur, mtr);
	if(index == clust_index)
		btr_pcur_copy_stored_position(cascade->pcur, pcur);
	else
		btr_pcur_store_position(cascade->pcur, mtr);

	mtr_commit(mtr);
	ut_a(cascade->pcur->rel_pos == BTR_PCUR_ON);
	cascade->state = UPD_NODE_UPDATE_CLUSTERED;

	err = row_update_cascade_for_mysql(thr, cascade, foreign->foreign_table);

	mtr_start(mtr);
	btr_pcur_restore_position(BTR_SEARCH_LEAF, pcur, mtr);
	return err;

nonstandard_exit_func:
	btr_pcur_store_position(pcur, mtr);

	mtr_commit(mtr);
	mtr_start(mtr);

	btr_pcur_restore_position(BTR_SEARCH_LEAF, pcur, mtr);

	return err;
}

/*对记录rec上设置一个S-LOCK事务锁*/
static ulint row_ins_set_shared_rec_lock(rec_t* rec, dict_index_t* index, que_thr_t* thr)
{
	ulint err;
	if(index->type & DICT_CLUSTERED)
		err = lock_clust_rec_read_check_and_lock(0, rec, index, LOCK_S, thr);
	else
		err = lock_sec_rec_read_check_and_lock(0, rec, index, LOCK_S, thr);

	return err;
}

/*插入记录时检查外键约束,约束成功或失败是通过对对应的记录行设置一个s-lock来决定的，
注意：在这个函数调用之前，调用者必须获得dict_foreign_key_check_lock的s-latch权*/
ulint row_ins_check_foreign_constraint(ibool check_ref, dict_foreign_t* foreign, dict_table_t* table, dict_index_t* index, dtuple_t* entry, que_thr_t* thr)
{
	dict_table_t*	check_table;
	dict_index_t*	check_index;
	ulint			n_fields_cmp;
	ibool           timeout_expired;
	rec_t*			rec;
	btr_pcur_t		pcur;
	ibool			moved;
	int				cmp;
	ulint			err;
	ulint			i;
	mtr_t			mtr;

run_again:
	ut_ad(rw_lock_own(&dict_foreign_key_check_lock, RW_LOCK_SHARED));
	/*用户session关闭了外键约束检查*/
	if(thr_get_trx(thr)->check_foreigns == FALSE)
		return DB_SUCCESS;

	for (i = 0; i < foreign->n_fields; i++) {
		if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i)))
			return(DB_SUCCESS);
	}

	if(check_ref){
		check_table = foreign->referenced_table;
		check_index = foreign->referenced_index;
	}
	else{
		check_table = foreign->foreign_table;
		check_index = foreign->foreign_index;
	}

	if(check_table == NULL){
		if(check_ref)
			return DB_NO_REFERENCED_ROW;
		else
			return DB_SUCCESS;
	}

	ut_a(check_table && check_index);
	if(check_table != table){
		/* We already have a LOCK_IX on table, but not necessarily on check_table */
		err = lock_table(0, check_table, LOCK_IS, thr);
		if(err != DB_SUCCESS)
			goto do_possible_lock_wait;
	}

	mtr_start(&mtr);

	/* Store old value on n_fields_cmp */
	n_fields_cmp = dtuple_get_n_fields_cmp(entry);
	dtuple_set_n_fields_cmp(entry, foreign->n_fields);
	btr_pcur_open(check_index, entry, PAGE_CUR_GE, BTR_SEARCH_LEAF, &pcur, &mtr);

	/* Scan index records and check if there is a matching record */
	for(;;){
		rec = btr_pcur_get_rec(&pcur);
		if (rec == page_get_infimum_rec(buf_frame_align(rec)))
			goto next_rec;
		/*对上层约束的记录添加一个s-lock事务锁，防止被改*/
		err = row_ins_set_shared_rec_lock(rec, check_index, thr);
		if(err != DB_SUCCESS)
			break;

		if (rec == page_get_supremum_rec(buf_frame_align(rec)))
			goto next_rec;

		cmp = cmp_dtuple_rec(entry, rec);
		if (cmp == 0) {
			if (!rec_get_deleted_flag(rec)) {
				if (check_ref) {			
					err = DB_SUCCESS;
					break;
				} 
				else if (foreign->type != 0) {
					err = row_ins_foreign_delete_or_set_null(thr, foreign, &pcur, &mtr);
					if (err != DB_SUCCESS) 
						break;
				} 
				else {
					err = DB_ROW_IS_REFERENCED;
					break;
				}
			}
		}

		if (cmp < 0) {
			if (check_ref)
				err = DB_NO_REFERENCED_ROW;
			else
				err = DB_SUCCESS;

			break;
		}
		ut_a(cmp == 0);

next_rec:
		moved = btr_pcur_move_to_next(&pcur, &mtr);
		if (!moved) {
			if (check_ref)			
				err = DB_NO_REFERENCED_ROW;
			else
				err = DB_SUCCESS;

			break;
		}
	}

	btr_pcur_close(&pcur);
	mtr_commit(&mtr);
	dtuple_set_n_fields_cmp(entry, n_fields_cmp);

do_possible_lock_wait:
	if (err == DB_LOCK_WAIT) { /*需要锁等待，进行线程挂起*/
		thr_get_trx(thr)->error_state = err;
		que_thr_stop_for_mysql(thr);

		timeout_expired = srv_suspend_mysql_thread(thr);
		if (!timeout_expired) /*没有事务锁超时*/
			goto run_again;

		err = DB_LOCK_WAIT_TIMEOUT;
	}

	return err;
}

/*为索引index检查外键约束*/
static ulint row_ins_check_foreign_constraints(dict_table_t* table, dict_index_t* index, dtuple_t* entry, que_thr_t* thr)
{
	dict_foreign_t*	foreign;
	ulint		err;
	trx_t*		trx;
	ibool		got_s_lock	= FALSE;

	trx = thr_get_trx(thr);
	foreign = UT_LIST_GET_FIRST(table->foreign_list);
	while(foreign){
		if(foreign->foreign_index == index){
			if (foreign->referenced_table == NULL) 
				foreign->referenced_table = dict_table_get(foreign->referenced_table_name, trx);

			if (!trx->has_dict_foreign_key_check_lock) {
				got_s_lock = TRUE;
				rw_lock_s_lock(&dict_foreign_key_check_lock);
				trx->has_dict_foreign_key_check_lock = TRUE;
			}
			/*检查外键约束索引的值的一致性*/
			err = row_ins_check_foreign_constraint(TRUE, foreign, table, index, entry, thr);
			if(got_s_lock){
				rw_lock_s_unlock(&dict_foreign_key_check_lock);	
				trx->has_dict_foreign_key_check_lock = FALSE;
			}

			if(err != DB_SUCCESS)
				return err;
		}

		foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
	}

	return DB_SUCCESS;
}

/*检查非聚集唯一性索引记录是否违反了唯一性原则（键值重复）,*/
static ulint row_ins_scan_sec_index_for_duplicate(dict_index_t* index, dtuple_t* entry, que_thr_t* thr)
{
	ulint		n_unique;
	ulint		i;
	int		cmp;
	ulint		n_fields_cmp;
	rec_t*		rec;
	btr_pcur_t	pcur;
	ulint		err		= DB_SUCCESS;
	ibool		moved;
	mtr_t		mtr;

	n_unique = dict_index_get_n_unique(index);

	for (i = 0; i < n_unique; i++) {
		if (UNIV_SQL_NULL == dfield_get_len(dtuple_get_nth_field(entry, i)))
			return(DB_SUCCESS);
	}

	mtr_start(&mtr);
	/*在索引树上进行查找比较*/
	n_fields_cmp = dtuple_get_n_fields_cmp(entry);
	dtuple_set_n_fields_cmp(entry, dict_index_get_n_unique(index));
	btr_pcur_open(index, entry, PAGE_CUR_GE, BTR_SEARCH_LEAF, &pcur, &mtr);

	for(;;){
		rec = btr_pcur_get_rec(&pcur);
		if (rec == page_get_infimum_rec(buf_frame_align(rec)))
			goto next_rec;

		err = row_ins_set_shared_rec_lock(rec, index, thr);
		if (err != DB_SUCCESS)
			break;

		if (rec == page_get_supremum_rec(buf_frame_align(rec)))
			goto next_rec;

		cmp = cmp_dtuple_rec(entry, rec);
		if(cmp == 0){
			if (row_ins_dupl_error_with_rec(rec, entry, index)){ /*键值重复*/
				err = DB_DUPLICATE_KEY; 
				thr_get_trx(thr)->error_info = index;
				break;
			}
		}
		if(cmp < 0) break;
next_rec:
		if(!btr_pcur_move_to_next(&pcur, &mtr))
			break;
	}

	mtr_commit(&mtr);
	dtuple_set_n_fields_cmp(entry, n_fields_cmp);

	return err;
}

/*检查聚集索引上的键值重复*/
static ulint row_ins_duplicate_error_in_clust(btr_cur_t* cursor, dtuple_t* entry, que_thr_t* thr, mtr_t* mtr)
{
	ulint	err;
	rec_t*	rec;
	page_t*	page;
	ulint	n_unique;

	trx_t*	trx	= thr_get_trx(thr);
	UT_NOT_USED(mtr);

	ut_a(cursor->index->type & DICT_CLUSTERED);
	ut_ad(cursor->index->type & DICT_UNIQUE);

	n_unique = dict_index_get_n_unique(cursor->index);
	if(cursor->low_match >= n_unique){
		rec = btr_cur_get_rec(cursor);
		page = buf_frame_align(rec);

		if(rec != page_get_infimum_rec(page)){
			/*设置s-lock,防止事务结束前变化*/
			err = row_ins_set_shared_rec_lock(rec, cursor->index, thr);
			if(err != DB_SUCCESS)
				return err;

			if (row_ins_dupl_error_with_rec(rec, entry, cursor->index)) { /*键值重复*/
				trx->error_info = cursor->index;
				return DB_DUPLICATE_KEY;
			}
		}
	}

	/*超过了匹配的列数，需要判断cursor对应的下一条记录*/
	if(cursor->up_match >= n_unique){
		rec = page_rec_get_next(btr_cur_get_rec(cursor));
		page = buf_frame_align(rec);
		if (rec != page_get_supremum_rec(page)){
			err = row_ins_set_shared_rec_lock(rec, cursor->index, thr);
			if(err != DB_SUCCESS)
				return err;

			if (row_ins_dupl_error_with_rec(rec, entry, cursor->index)) { /*键值重复*/
				trx->error_info = cursor->index;
				return(DB_DUPLICATE_KEY);
			}
		}

		ut_a(!(cursor->index->type & DICT_CLUSTERED));
	}

	return DB_SUCCESS;
}

/*判断是修改一个记录还是插入一个新的记录*/
UNIV_INLINE ulint row_ins_must_modify(btr_cur_t* cursor)
{
	ulint enough_match;
	rec_t* rec;
	page_t* page;

	/*确定index匹配的列数*/
	enough_match = dict_index_get_n_unique_in_tree(cursor->index);
	/*匹配到了记录*/
	/* NOTE: (compare to the note in row_ins_duplicate_error) Because node
	pointers on upper levels of the B-tree may match more to entry than
	to actual user records on the leaf level, we have to check if the
	candidate record is actually a user record. In a clustered index
	node pointers contain index->n_unique first fields, and in the case
	of a secondary index, all fields of the index. */
	if (cursor->low_match >= enough_match) {
		rec = btr_cur_get_rec(cursor);
		page = buf_frame_align(rec);
		if (rec != page_get_infimum_rec(page)) /*确定是要修改*/
			return ROW_INS_PREV;
	}

	return 0;
}




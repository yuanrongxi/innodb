#include "row0purge.h"

#include "fsp0fsp.h"
#include "mach0data.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0roll.h"
#include "trx0undo.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "que0que.h"
#include "row0row.h"
#include "row0upd.h"
#include "row0vers.h"
#include "log0log.h"

/*创建一个purge que node*/
purge_node_t* row_purge_node_create(que_thr_t* parent, mem_heap_t* heap)
{
	purge_node_t* node;
	
	ut_ad(parent && heap);

	node = mem_heap_alloc(heap, sizeof(purge_node_t));
	node->common.type = QUE_NODE_PURGE;
	node->common.parent = parent;
	node->heap = heap;

	return node;
}

/*将purge node的pcur复位到node->ref所在的聚集索引树的位置*/
static ibool row_purge_reposition_pcur(ulint mode, purge_node_t* node, mtr_t* mtr)
{
	ibool	found;

	if(node->found_clust)
		return btr_pcur_restore_position(mode, &(node->pcur), mtr);
	
	found = row_search_on_row_ref(&(node->pcur), mode, node->table, node->ref, mtr);
	node->found_clust = found;
	if(found)
		btr_pcur_store_position(&(node->pcur), mtr);

	return found;
}

/*将聚集索引上标识为delete marked的记录删除*/
static ibool row_purge_remove_clust_if_poss_low(purge_node_t* node, que_thr_t* thr, ulint mode)
{
	dict_index_t*	index;
	btr_pcur_t*		pcur;
	btr_cur_t*		btr_cur;
	ibool			success;
	ulint			err;
	mtr_t			mtr;

	UT_NOT_USED(thr);
	/*获得node对应表的聚集索引和pcur对应的btree游标*/
	index = dict_table_get_first_index(node->table);
	pcur = &(node->pcur);
	btr_cur = btr_pcur_get_btr_cur(pcur);

	mtr_start(&mtr);

	/*将purge node的pcur复位到node->ref所在的聚集索引树的位置*/
	success = row_purge_reposition_pcur(mode, node, &mtr);
	if(!success){ /*pcur复位失败，记录已经被删除，直接返回*/
		btr_pcur_commit_specify_mtr(pcur, &mtr);
		return TRUE;
	}

	/*不能删除，记录做了修改*/
	if(ut_dulint_cmp(node->roll_ptr, row_get_rec_roll_ptr(btr_pcur_get_rec(pcur), index)) != 0){
		btr_pcur_commit_specify_mtr(pcur, &mtr);
		return TRUE;
	}

	/*直接在聚集索引树上进行记录的物理删除*/
	if(mode == BTR_MODIFY_LEAF)
		success = btr_cur_optimistic_delete(btr_cur, &mtr);
	else{
		ut_ad(mode == BTR_MODIFY_TREE);
		btr_cur_pessimistic_delete(&err, FALSE, btr_cur, FALSE, &mtr);

		if(err == DB_SUCCESS)
			success = TRUE;
		else if(err == DB_OUT_OF_FILE_SPACE)
			success = FALSE;
		else
			ut_a(0);
	}

	btr_pcur_commit_specify_mtr(pcur, &mtr);
	return success;
}

static void row_purge_remove_clust_if_poss(purge_node_t* node, que_thr_t* thr)
{
	ibool success;
	ulint n_tries = 0;

	/*尝试从btree叶子中删除记录*/
	success = row_purge_remove_clust_if_poss_low(node, thr, BTR_MODIFY_LEAF);
	if(success)
		return ;

	/*叶子无法删除直接删除，需要做悲观式删除，要删除的记录夸页了！！需要修改表空间space*/
retry:
	success = row_purge_remove_clust_if_poss_low(node, thr, BTR_MODIFY_TREE);
	if (!success && n_tries < BTR_CUR_RETRY_DELETE_N_TIMES){ /*表空间正在cleaning,无法同时进行，需要做等待*/
		n_tries++;
		os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);

		goto retry;
	}

	ut_a(success);
}

/*purge过程删除一个辅助索引上的entry*/
static ibool row_purge_remove_sec_if_poss_low(purge_node_t* node, que_thr_t* thr, dict_index_t* index, dtuple_t* entry, ulint mode)
{
	btr_pcur_t	pcur;
	btr_cur_t*	btr_cur;
	ibool		success;
	ibool		old_has;
	ibool		found;
	ulint		err;
	mtr_t		mtr;
	mtr_t*		mtr_vers;

	UT_NOT_USED(thr);

	/*检查是否可以做页刷盘*/
	log_free_check();

	mtr_start(&mtr);
	/*在index对应的索引树上找到entry对应的pcur位置*/
	found = row_search_index_entry(index, entry, mode, &pcur, &mtr);
	if(!found){
		btr_pcur_close(&pcur);
		mtr_commit(&mtr);
		return TRUE;
	}

	btr_cur = btr_pcur_get_btr_cur(&pcur);
	
	mtr_vers = mem_alloc(size_of(mtr_t));
	mtr_start(mtr_vers);

	success = row_purge_reposition_pcur(BTR_SEARCH_LEAF, node, mtr_vers);
	if(success){
		/*查询聚集索引上是否有有效的记录（正在使用的记录就是有效的）*/
		old_has = row_vers_old_has_index_entry(TRUE, btr_pcur_get_rec(&(node->pcur)), mtr_vers, index, entry);
	}

	btr_pcur_commit_specify_mtr(&(node->pcur), mtr_vers);
	mem_free(mtr_vers);

	if(!success || !old_has){ /*索引上定位不到记录或者记录在聚集索引上无效了，可以进行删除*/
		if (mode == BTR_MODIFY_LEAF)	
			success = btr_cur_optimistic_delete(btr_cur, &mtr);
		else {
			ut_ad(mode == BTR_MODIFY_TREE);
			btr_cur_pessimistic_delete(&err, FALSE, btr_cur, FALSE, &mtr);
			if (err == DB_SUCCESS)
				success = TRUE;
			else if (err == DB_OUT_OF_FILE_SPACE)
				success = FALSE;
			else
				ut_a(0);
		}
	}
}

/*删除已经失效的辅助索引上的记录*/
UNIV_INLINE void row_purge_remove_sec_if_poss(purge_node_t* node, que_thr_t* thr, dict_index_t* index, dtuple_t* entry)
{
	ibool	success;
	ulint	n_tries		= 0;

	/*先尝试从叶子节点上乐观删除*/
	success = row_purge_remove_sec_if_poss_low(node, thr, index, entry, BTR_MODIFY_LEAF);
	if(success)
		return ;

retry:
	/*叶子无法删除直接删除，需要做悲观式删除，要删除的记录夸页了！！需要修改表空间space*/
	success = row_purge_remove_sec_if_poss_low(node, thr, index, entry, BTR_MODIFY_TREE);
	if(!success && n_tries < BTR_CUR_RETRY_DELETE_N_TIMES){
		n_tries ++;
		os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);

		goto retry;
	}

	ut_a(success);
}

/*purge过程删除整个行（聚集索引记录和辅助索引记录）*/
static void row_purge_del_mark(purge_node_t* node, que_thr_t* thr)
{
	mem_heap_t*	heap;
	dtuple_t*	entry;
	dict_index_t*	index;

	ut_ad(node && thr);

	heap = mem_heap_create(1024);
	/*先删除此行（row)所有对应的辅助索引*/
	while(node->index != NULL){
		/*构建一个index索引匹配的tuple记录*/
		entry = row_build_index_entry(node->row, index, heap);
		row_purge_remove_sec_if_poss(node, thr, index, entry);
		node->index = dict_table_get_next_index(node->index);
	}

	mem_heap_free(heap);
	
	/*再删除聚集索引上的所有记录*/
	row_purge_remove_clust_if_poss(node, thr);
}

/*删除一个修改行的历史版本*/
static void row_purge_upd_exist_or_extern(purge_node_t* node, que_thr_t* thr)
{
	mem_heap_t*		heap;
	dtuple_t*		entry;
	dict_index_t*	index;
	upd_field_t*	ufield;
	ibool			is_insert;
	ulint			rseg_id;
	ulint			page_no;
	ulint			offset;
	ulint			internal_offset;
	byte*			data_field;
	ulint			data_field_len;
	ulint			i;
	mtr_t			mtr;

	ut_ad(node && thr);

	if(node->rec_type == TRX_UNDO_UPD_DEL_REC)
		goto skip_secondaries;

	heap = mem_heap_create(1024);
	/*删除作废的辅助索引记录*/
	while(node->index != NULL){
		index = node->index;
		if (row_upd_changes_ord_field_binary(NULL, node->index, node->update)){ /*删除upd field对应被表示为del mark的二级索引*/
			entry = row_build_index_entry(node->row, index, heap);
			row_purge_remove_sec_if_poss(node, thr, index, entry);
		}

		node->index = dict_table_get_next_index(node->index);
	}

	mem_heap_free(heap);

skip_secondaries:
	for(i = 0; i < upd_get_n_fields(node->update); i++){
		ufield = upd_get_nth_field(node->update, i);

		if(ufield->extern_storage){
			/* We use the fact that new_val points to node->undo_rec and get thus the offset of
			dfield data inside the unod record. Then we can calculate from node->roll_ptr the file
			address of the new_val data */

			internal_offset = ((byte*)ufield->new_val.data) - node->undo_rec;
			ut_a(internal_offset < UNIV_PAGE_SIZE);
			/*通过roll ptr得到历史记录存储的位置*/
			trx_undo_decode_roll_ptr(node->roll_ptr, &is_insert, &rseg_id, &page_no, &offset);

			mtr_start(&mtr);

			/*获得table的聚集索引*/
			index = dict_table_get_first_index(node->table);
			mtr_x_lock(dict_tree_get_lock(index->tree), &mtr);
			/*这里其实是对页的历史版本修改，需要进行聚集索引的BTREE上x-latch*/
			btr_root_get(index->tree, &mtr);

			data_field = buf_page_get(0, page_no, RW_X_LATCH, &mtr) + offset + internal_offset;
			buf_page_dbg_add_level(buf_frame_align(data_field), SYNC_TRX_UNDO_PAGE);

			data_field_len = ufield->new_val.len;

			btr_free_externally_stored_field(index, data_field, data_field_len, FALSE, &mtr);
			mtr_commit(&mtr);
		}
	}
}

/*对undo rec的解析，并将解析的结果存入node对应的各个值上,这个应该是为了purge过程做准备*/
static ibool row_purge_parse_undo_rec(purge_node_t* node, ibool* updated_extern, que_thr_t* thr)
{
	dict_index_t*	clust_index;
	byte*		ptr;
	dulint		undo_no;
	dulint		table_id;
	dulint		trx_id;
	dulint		roll_ptr;
	ulint		info_bits;
	ulint		type;
	ulint		cmpl_info;

	ut_ad(node && thr);

	/*读取undo rec的头部*/
	ptr = trx_undo_rec_get_pars(node->undo_rec, &type, &cmpl_info, updated_extern, &undo_no, &table_id);
	node->rec_type = type;
	if(type == TRX_UNDO_UPD_DEL_REC && !(*updated_extern))
		return FALSE;

	/*读取undo update rec特有的头信息（bits, trx_id, roll_ptr)三个值*/
	ptr = trx_undo_update_rec_get_sys_cols(ptr, &trx_id, &roll_ptr, &info_bits);
	node->table = NULL;

	/* Purge requires no changes to indexes: we may return */
	if(type == TRX_UNDO_UPD_EXIST_REC && (cmpl_info & UPD_NODE_NO_ORD_CHANGE) && !(*updated_extern)){
		return FALSE;
	}

	mutex_enter(&(dict_sys->mutex));
	/*对表对象的读取*/
	node->table = dict_table_get_on_id(table_id, thr_get_trx(thr));
	rw_lock_x_lock(&(purge_sys->purge_is_running));

	mutex_exit(&(dict_sys->mutex));

	if (node->table == NULL){
		rw_lock_x_unlock(&(purge_sys->purge_is_running));
		return FALSE;
	}

	/*获得聚集索引对象*/
	clust_index = dict_table_get_first_index(node->table);
	if(clust_index == NULL){ /*聚集索引不存在？？*/
		rw_lock_x_unlock(&(purge_sys->purge_is_running));
		return FALSE;
	}

	/*构建一个聚集索引的索引参考记录（node->ref）*/
	ptr = trx_undo_rec_get_row_ref(ptr, clust_index, &(node->ref), node->heap);
	/*获得update vector*/
	ptr = trx_undo_update_rec_get_update(ptr, clust_index, type, trx_id, roll_ptr, info_bits, node->heap, &(node->update));

	if(!cmpl_info & UPD_NODE_NO_ORD_CHANGE){
		/*从undo update rec(ptr)中读读取一行记录，并存储在row中*/
		ptr = trx_undo_rec_get_partial_row(ptr, clust_index, &(node->row), node->heap);
	}

	return TRUE;
}

/*对undo rec进行purge*/
static ulint row_purge(purge_node_t* node, que_thr_t* thr)
{
	dulint	roll_ptr;
	ibool	purge_needed;
	ibool	updated_extern;

	ut_ad(node && thr);

	/*获得一条purge undo rec*/
	node->undo_rec = trx_purge_fetch_next_rec(&roll_ptr, &(node->reservation), node->heap);
	if(node->undo_rec == NULL){
		thr->run_node = que_node_get_parent(node);
		return DB_SUCCESS;
	}

	node->roll_ptr = roll_ptr;
	if(node->undo_rec == &trx_purge_dummy_rec)
		purge_needed = FALSE;
	else /*对undo updage rec记录分析*/
		purge_needed = row_purge_parse_undo_rec(node, &updated_extern, thr);

	if(purge_needed){
		node->found_clust = FALSE;
		node->index = dict_table_get_next_index(dict_table_get_first_index(node->table)); /*获得索引列表中的第一个辅助索引*/

		if(node->rec_type == TRX_UNDO_DEL_MARK_REC) /*删除del mark的记录*/
			row_purge_del_mark(node, thr);
		else if(updated_extern || node->rec_type == TRX_UNDO_UPD_EXIST_REC) /*删除修改后的历史版本记录*/
			row_purge_upd_exist_or_extern(node, thr);

		/*关闭索引树游标，在数据定位的时候会打开row_purge_reposition_pcur*/
		if(node->found_clust)
			btr_pcur_close(&(node->pcur));

		rw_lock_x_unlock(&(purge_sys->purge_is_running));		
	}

	trx_purge_rec_release(node->reservation);
	mem_heap_empty(node->heap);
	
	thr->run_node = node;

	return DB_SUCCESS;
}

/*进行purge过程操作*/
que_thr_t* row_purge_step(que_thr_t* thr)
{
	purge_node_t*	node;
	ulint		err;

	ut_ad(thr);
	node = thr->run_node;

	ut_ad(que_node_get_type(node) == QUE_NODE_PURGE);
	err = row_purge(node, thr);
	ut_ad(err == DB_SUCCESS);

	return(thr);
}



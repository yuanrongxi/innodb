#include "btr0cur.h"

#include "page0page.h"
#include "rem0rec.h"
#include "rem0cmp.h"
#include "btr0btr.h"
#include "btr0sea.h"
#include "row0upd.h"
#include "trx0rec.h"
#include "que0que.h"
#include "row0row.h"
#include "srv0srv.h"
#include "ibuf0ibuf.h"
#include "lock0lock.h"

ibool	btr_cur_print_record_ops = FALSE;

ulint	btr_cur_rnd = 0;

ulint	btr_cur_n_non_sea = 0;
ulint	btr_cur_n_sea = 0;
ulint	btr_cur_n_non_sea_old = 0;
ulint	btr_cur_n_sea_old = 0;


/*页重组因子*/
#define BTR_CUR_PAGE_REORGANIZE_LIMIT	(UNIV_PAGE_SIZE / 32)

#define BTR_KEY_VAL_ESTIMATE_N_PAGES	8

/*BLOB列的头结构*/
#define BTR_BLOB_HDR_PART_LEN			0	/*blob在对应页中的长度*/
#define BTR_BLOB_HDR_NEXT_PAGE_NO		4	/*下一个存有blob数据的page no*/
#define BTR_BLOB_HDR_SIZE				8	


static void			btr_cur_add_path_info(btr_cur_t* cursor, ulint height, ulint root_height);
static void			btr_rec_free_updated_extern_fields(dict_index_t* index, rec_t* rec, upd_t* update, ibool do_not_free_inherited, mtr_t* mtr);
static ulint		btr_rec_get_externally_stored_len(rec_t* rec);

/*对page做互斥latch并发*/
static void btr_cur_latch_leaves(dict_tree_t* tree, page_t* page, ulint space, ulint page_no, ulint latch_mode, btr_cur_t* cursor, mtr_t* mtr)
{
	ulint	left_page_no;
	ulint	right_page_no;

	ut_ad(tree && page && mtr);

	if(latch_mode == BTR_SEARCH_LEAF) /*查找时，可以直接获取一个S-LATCH,*/
		btr_page_get(space, page_no, RW_S_LATCH, mtr);
	else if(latch_mode == BTR_MODIFY_LEAF) /*叶子节点修改，直接对叶子节点x-latch即可*/
		btr_page_get(space, page_no, RW_X_LATCH, mtr);
	else if(latch_mode == BTR_MODIFY_TREE){ /*分别会获取前后两页和自己的x-latch*/
		left_page_no = btr_page_get_prev(page, mtr);
		if(left_pge_no != FIL_NULL) /*对前一页获取一个X-LATCH*/
			btr_page_get(space, left_page_no, RW_X_LATCH, mtr);

		btr_page_get(space, page_no, RW_X_LATCH, mtr);

		/*对后一个页也获取一个X-LATCH*/
		right_page_no = btr_page_get_next(page, mtr);
		if(right_page_no != FIL_NULL)
			btr_page_get(space, right_page_no, RW_X_LATCH, mtr);
	}
	else if(latch_mode == BTR_SEARCH_PREV){ /*搜索前一页，对前一页加上一个s-latch*/
		left_page_no = btr_page_get_prev(page, mtr);
		if(left_page_no != FIL_NULL)
			cursor->left_page = btr_page_get(space, left_page_no, RW_S_LATCH, mtr);

		btr_page_get(space, page_no, RW_S_LATCH, mtr);
	}
	else if(latch_mode == BTR_MODIFY_PREV){ /*更改前一页，对前一页加上一个x-latch*/
		left_page_no = btr_page_get_prev(page, mtr);
		if(left_page_no != FIL_NULL)
			cursor->left_page = btr_page_get(space, left_page_no, RW_X_LATCH, mtr);

		btr_page_get(space, page_no, RW_X_LATCH, mtr);
	}
	else
		ut_error;
}

void btr_cur_search_to_nth_level(dict_index_t* index, ulint level, dtuple_t* tuple, ulint mode, ulint latch_mode,
	btr_cur_t* cursor, ulint has_search_latch, mtr_t* mtr)
{
	dict_tree_t*	tree;
	page_cur_t*	page_cursor;
	page_t*		page;
	page_t*		guess;
	rec_t*		node_ptr;
	ulint		page_no;
	ulint		space;
	ulint		up_match;
	ulint		up_bytes;
	ulint		low_match;
	ulint 		low_bytes;
	ulint		height;
	ulint		savepoint;
	ulint		rw_latch;
	ulint		page_mode;
	ulint		insert_planned;
	ulint		buf_mode;
	ulint		estimate;
	ulint		ignore_sec_unique;
	ulint		root_height;

	btr_search_t* info;

	ut_ad(level == 0 || mode == PAGE_CUR_LE);
	ut_ad(dict_tree_check_search_tuple(index->tree, tuple));
	ut_ad(!(index->type & DICT_IBUF) || ibuf_inside());
	ut_ad(dtuple_check_typed(tuple));

	insert_planned = latch_mode & BTR_INSERT;
	estimate = latch_mode & BTR_ESTIMATE;			/*预估计算的动作*/
	ignore_sec_unique = latch_mode & BTR_IGNORE_SEC_UNIQUE;
	latch_mode = latch_mode & ~(BTR_INSERT | BTR_ESTIMATE | BTR_IGNORE_SEC_UNIQUE);

	ut_ad(!insert_planned || mode == PAGE_CUR_LE);

	cursor->flag = BTR_CUR_BINARY;
	cursor->index = index;

	/*在自适应HASH表中查找*/
	info = btr_search_get_info(index);
	guess = info->root_guess;

#ifdef UNIV_SEARCH_PERF_STAT
	info->n_searches++;
#endif

	/*在自适应hash中找到了对应的记录*/
	if (btr_search_latch.writer == RW_LOCK_NOT_LOCKED
		&& latch_mode <= BTR_MODIFY_LEAF && info->last_hash_succ
		&& !estimate
		&& btr_search_guess_on_hash(index, info, tuple, mode, latch_mode, cursor, has_search_latch, mtr)) {

			ut_ad(cursor->up_match != ULINT_UNDEFINED || mode != PAGE_CUR_GE);
			ut_ad(cursor->up_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
			ut_ad(cursor->low_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
			btr_cur_n_sea++;

			return;
	}

	btr_cur_n_sea ++;

	if(has_search_latch)
		rw_lock_s_unlock(&btr_search_latch);

	/*获得mtr 的保存数据长度*/
	savepoint = mtr_set_savepoint(mtr);

	tree = index->tree;

	/*获取一个x-latch,更改有可能会更改索引*/
	if(latch_mode == BTR_MODIFY_TREE)
		mtr_x_lock(dict_tree_get_lock(tree), mtr);
	else if(latch_mode == BTR_CONT_MODIFY_TREE) 
		ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));
	else /*对tree 索引获取一个s-latch*/
		mtr_s_lock(dict_tree_get_lock(tree), mtr);

	page_cursor = btr_cur_get_page_cur(cursor);
	space = dict_tree_get_space(tree);
	page_no = dict_tree_get_page(tree);

	up_match = 0;
	up_bytes = 0;
	low_match = 0;
	low_bytes = 0;

	height = ULINT_UNDEFINED;
	rw_latch = RW_NO_LATCH;
	buf_mode = BUF_GET;

	/*确定page记录的匹配模式*/
	if (mode == PAGE_CUR_GE)
		page_mode = PAGE_CUR_L;
	else if (mode == PAGE_CUR_G)
		page_mode = PAGE_CUR_LE;
	else if (mode == PAGE_CUR_LE)
		page_mode = PAGE_CUR_LE;
	else {
		ut_ad(mode == PAGE_CUR_L);
		page_mode = PAGE_CUR_L;
	}

	for(;;){
		if(height == 0 && latch_mode <= BTR_MODIFY_LEAF){
			rw_latch = latch_mode;
			/*尝试将page插入到ibuffer当中*/
			if(insert_planned && ibuf_should_try(index, ignore_sec_unique))
				buf_mode = BUF_GET_IF_IN_POOL;
		}

retry_page_get:
		page = buf_page_get_gen(space, page_no, rw_latch, guess, buf_mode, IB__FILE__, __LINE__, mtr);
		if(page == NULL){
			ut_ad(buf_mode == BUF_GET_IF_IN_POOL);
			ut_ad(insert_planned);
			ut_ad(cursor->thr);

			/*page不能插入到insert buffer中,将会重试,知道page插入到ibuf中*/
			if(ibuf_should_try(index, ignore_sec_unique) && ibuf_insert(tuple, index, space, page_no, cursor->thr)){
				cursor->flag = BTR_CUR_INSERT_TO_IBUF;
				return ;
			}

			buf_mode = BUF_GET;
			goto retry_page_get;
		}

		ut_ad(0 == ut_dulint_cmp(tree->id, btr_page_get_index_id(page)));
		if(height == ULINT_UNDEFINED){
			height = btr_page_get_level(page, mtr);
			root_height = height;
			cursor->tree_height = root_height + 1;

			if(page != guess)
				info->root_guess = page;
		}

		if(height == 0){
			if(rw_latch == RW_NO_LATCH)
				btr_cur_latch_leaves(tree, page, space, page_no, latch_mode, cursor, mtr);
			/*释放savepoint以下的mtr latch*/
			if(latch_mode != BTR_MODIFY_TREE && latch_mode != BTR_CONT_MODIFY_TREE)
				mtr_release_s_latch_at_savepoint(mtr, savepoint, dict_tree_get_lock(tree));

			page_mode = mode;
		}
		/*在页中进行二分查找对应的记录,这个只是找page node ptr记录*/
		page_cur_search_with_match(page, tuple, page_mode, &up_match, &up_bytes, &low_match, &low_bytes, page_cursor);
		if(estimate) /*估算row数*/
			btr_cur_add_path_info(cursor, height, root_height);

		if(level == height){ /*已经找到对应的层了，不需要深入更低层上*/
			if(level > 0)
				btr_page_get(space, page_no, RW_X_LATCH, mtr);
			break;
		}

		ut_ad(height > 0);

		height --;
		guess = NULL;

		node_ptr = page_cur_get_rec(page_cursor);
		/*获取孩子节点的page no*/
		page_no = btr_node_ptr_get_child_page_no(node_ptr);
	}

	if(level == 0){
		cursor->low_match = low_match;
		cursor->low_bytes = low_bytes;
		cursor->up_match = up_match;
		cursor->up_bytes = up_bytes;
		/*更新自适应HASH索引*/
		btr_search_info_update(index, cursor);

		ut_ad(cursor->up_match != ULINT_UNDEFINED || mode != PAGE_CUR_GE);
		ut_ad(cursor->up_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
		ut_ad(cursor->low_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
	}

	if(has_search_latch)
		rw_lock_s_lock(&btr_search_latch);
}

/*将btree cursor定位到index索引范围的开始或者末尾，from_left = TRUE，表示定位到最前面*/
void btr_cur_open_at_index_side(ibool from_left, dict_index_t* index, ulint latch_mode, btr_cur_t* cursor, mtr_t* mtr)
{
	page_cur_t*	page_cursor;
	dict_tree_t*	tree;
	page_t*		page;
	ulint		page_no;
	ulint		space;
	ulint		height;
	ulint		root_height;
	rec_t*		node_ptr;
	ulint		estimate;
	ulint       savepoint;

	estimate = latch_mode & BTR_ESTIMATE;
	latch_mode = latch_mode & ~BTR_ESTIMATE;

	tree = index->tree;

	savepoint = mtr_set_savepoint(mtr);
	if(latch_mode == BTR_MODIFY_TREE)
		mtr_x_lock(dict_tree_get_lock(tree), mtr);
	else
		mtr_s_lock(dict_tree_get_lock(tree), mtr);

	page_cursor = btr_cur_get_page_cur(cursor);
	cursor->index = index;

	space = dict_tree_get_space(tree);
	page_no = dict_tree_get_page(tree);

	height = ULINT_UNDEFINED;
	for(;;){
		page = buf_page_get_gen(space, page_no, RW_NO_LATCH, NULL,
			BUF_GET, IB__FILE__, __LINE__, mtr);

		ut_ad(0 == ut_dulint_cmp(tree->id, btr_page_get_index_id(page)));

		if(height == ULINT_UNDEFINED){
			height = btr_page_get_level(page, mtr);
			root_height = height;
		}

		if(height == 0){
			btr_cur_latch_leaves(tree, page, space, page_no, latch_mode, cursor, mtr);

			if(latch_mode != BTR_MODIFY_TREE && latch_mode != BTR_CONT_MODIFY_TREE)
				mtr_release_s_latch_at_savepoint(mtr, savepoint, dict_tree_get_lock(tree));
		}

		if(from_left) /*从页第一条记录*/
			page_cur_set_before_first(page, page_cursor);
		else /*从页的最后一条记录开始*/
			page_cur_set_after_last(page, page_cursor);

		if(height == 0){
			if(estimate)
				btr_cur_add_path_info(cursor, height, root_height);
			break;
		}

		ut_ad(height > 0);

		if(from_left)
			page_cur_move_to_next(page_cursor);
		else
			page_cur_move_to_prev(page_cursor);

		if(estimate)
			btr_cur_add_path_info(cursor, height, root_height);

		height --;
		node_ptr = page_cur_get_rec(page_cursor);

		page_no = btr_node_ptr_get_child_page_no(node_ptr);
	}
}

/*在btree index的管辖范围，随机定位到一个位置*/
void btr_cur_open_at_rnd_pos(dict_index_t* index, ulint latch_mode, btr_cur_t* cursor, mtr_t* mtr)
{
	page_cur_t*	page_cursor;
	dict_tree_t*	tree;
	page_t*		page;
	ulint		page_no;
	ulint		space;
	ulint		height;
	rec_t*		node_ptr;

	tree = index->tree;
	if(latch_mode == BTR_MODIFY_TREE)
		mtr_x_lock(dict_tree_get_lock(tree), mtr);
	else
		mtr_s_lock(dict_tree_get_lock(tree), mtr);

	page_cursor = btr_cur_get_page_cur(cursor);
	cursor->index = index;

	space = dict_tree_get_space(tree);
	page_no = dict_tree_get_page(tree);

	height = ULINT_UNDEFINED;
	for(;;){
		page = buf_page_get_gen(space, page_no, RW_NO_LATCH, NULL, BUF_GET, IB__FILE__, __LINE__, mtr);
		ut_ad(0 == ut_dulint_cmp(tree->id, btr_page_get_index_id(page)));

		if(height == ULINT_UNDEFINED)
			height = btr_page_get_level(page, mtr);

		if(height == 0)
			btr_cur_latch_leaves(tree, page, space, page_no, latch_mode, cursor, mtr);
		/*随机定位一条记录，并将page cursor指向它*/
		page_cur_open_on_rnd_user_rec(page, page_cursor);	
		if(height == 0)
			break;

		ut_ad(height > 0);
		height --;

		node_ptr = page_cur_get_rec(page_cursor);
		page_no = btr_node_ptr_get_child_page_no(node_ptr);
	}
}

static rec_t* btr_cur_insert_if_possible(btr_cur_t* cursor, dtuple_t* tuple, ibool* reorg, mtr_t* mtr)
{
	page_cur_t*	page_cursor;
	page_t*		page;
	rec_t*		rec;

	ut_ad(dtuple_check_typed(tuple));

	*reorg = FALSE;

	page = btr_cur_get_page(cursor);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	page_cursor = btr_cur_get_page_cur(cursor);

	/*将tuple插入到page中*/
	rec = page_cur_tuple_insert(page_cursor, tuple, mtr);
	if(rec == NULL){
		/*可能空间不够或者排序不对，进行页重组,有可能插入记录的空间无法在page上分配(没有空闲的rec空间，删除的单条记录 < tuple所需要的空间)*/
		btr_page_reorganize(page, mtr);
		*reorg = TRUE;
		/*重新定位page游标*/
		page_cur_search(page, tuple, PAGE_CUR_GE, page_cursor);

		rec = page_cur_tuple_insert(page_cursor, tuple, mtr);
	}

	return rec;
}

/*为插入的记录添加一个事务锁和回滚日志*/
UNIV_INLINE ulint btr_cur_ins_lock_and_undo(ulint flags, btr_cur_t* cursor, dtuple_t* entry, que_thr_t* thr, ibool* inherit)
{
	dict_index_t*	index;
	ulint		err;
	rec_t*		rec;
	dulint		roll_ptr;

	rec = btr_cur_get_rec(cursor);
	index = cursor->index;

	/*添加一个事务锁，如果有对应的行锁，重复利用*/
	err = lock_rec_insert_check_and_lock(flags, rec, index, thr, inherit);
	if(err != DB_SUCCESS) /*添加锁失败*/
		return err;

	if(index->type & DICT_CLUSTERED && !(index->type & DICT_IBUF)){
		err = trx_undo_report_row_operation(flags, TRX_UNDO_INSERT_OP, thr, index, entry,
							NULL, 0, NULL, &roll_ptr);

		if(err != DB_SUCCESS)
			return err;

		if(!(flags & BTR_KEEP_SYS_FLAG))
			row_upd_index_entry_sys_field(entry, index, DATA_ROLL_PTR, roll_ptr);
	}

	return DB_SUCCESS;
}

/*尝试以乐观式插入记录*/
ulint btr_cur_optimistic_insert(ulint flags, btr_cur_t* cursor, dtuple_t* entry, rec_t** rec, big_rec_t** big_rec, que_thr_t* thr, mtr_t* mtr)
{
	big_rec_t*	big_rec_vec	= NULL;
	dict_index_t*	index;
	page_cur_t*	page_cursor;
	page_t*		page;
	ulint		max_size;
	rec_t*		dummy_rec;
	ulint		level;
	ibool		reorg;
	ibool		inherit;
	ulint		rec_size;
	ulint		data_size;
	ulint		extra_size;
	ulint		type;
	ulint		err;

	*big_rec = NULL;

	page = btr_cur_get_page(cursor);
	index = cursor->index;

	if(!dtuple_check_typed_no_assert(entry))
		fprintf(stderr, "InnoDB: Error in a tuple to insert into table %lu index %s\n",
			index->table_name, index->name);

	if (btr_cur_print_record_ops && thr) {
		printf("Trx with id %lu %lu going to insert to table %s index %s\n",
			ut_dulint_get_high(thr_get_trx(thr)->id),
			ut_dulint_get_low(thr_get_trx(thr)->id),
			index->table_name, index->name);

		dtuple_print(entry);
	}

	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	/*page可以容纳的最大记录空间*/
	max_size = page_get_max_insert_size_after_reorganize(page, 1);
	level = btr_page_get_level(page, mtr); /*获得page所处的层*/

calculate_sizes_again:
	/*获得tuple存储需要的空间大小*/
	data_size = dtuple_get_data_size(entry);
	/*获得rec header需要的长度*/
	extra_size = rec_get_converted_extra_size(data_size, dtuple_get_n_fields(entry));
	rec_size = data_size + extra_size;

	/*记录超出最大的存储范围，将其转化为big_rec*/
	if((rec_size >= page_get_free_space_of_empty() / 2) || rec_size >= REC_MAX_DATA_SIZE){
		big_rec_vec = dtuple_convert_big_rec(index, entry, NULL, 0);

		/*转化失败,记录实在太大,可能有太多的短列？*/
		if(big_rec_vec == NULL)
			return DB_TOO_BIG_RECORD;

		goto calculate_sizes_again;
	}

	type = index->type;

	/*聚集索引，在叶子节点可以进行左右分裂,而且BTREE树的空间不够？？不能存入，将转化为big_rec的操作回滚*/
	if ((type & DICT_CLUSTERED)
		&& (dict_tree_get_space_reserve(index->tree) + rec_size > max_size)
		&& (page_get_n_recs(page) >= 2)
		&& (0 == level)
		&& (btr_page_get_split_rec_to_right(cursor, &dummy_rec)
		|| btr_page_get_split_rec_to_left(cursor, &dummy_rec))){
			if(big_rec_vec) /*将tuple转化成big_rec*/
				dtuple_convert_back_big_rec(index, entry, big_rec_vec);

			return DB_FAIL;
	}

	if (!(((max_size >= rec_size) && (max_size >= BTR_CUR_PAGE_REORGANIZE_LIMIT))
		|| page_get_max_insert_size(page, 1) >= rec_size || page_get_n_recs(page) <= 1)) {
			if(big_rec_vec)
				dtuple_convert_back_big_rec(index, entry, big_rec_vec);

			return DB_FAIL;
	}

	err = btr_cur_ins_lock_and_undo(flags, cursor, entry, thr, &inherit);
	if(err != DB_SUCCESS){ /*事务加锁不成功，回滚big_rec*/
		if(big_rec_vec)
			dtuple_convert_back_big_rec(index, entry, big_rec_vec);

		return err;
	}

	page_cursor = btr_cur_get_page_cur(cursor);
	reorg = FALSE;

	/*将tuple插入到page当中*/
	*rec = page_cur_insert_rec_low(page_cursor, entry, data_size, NULL, mtr);
	if(*rec == NULL){ /*插入失败，进行page重组*/
		btr_page_reorganize(page, mtr);

		ut_ad(page_get_max_insert_size(page, 1) == max_size);
		reorg = TRUE;

		page_cur_search(page, entry, PAGE_CUR_LE, page_cursor);
		*rec = page_cur_tuple_insert(page_cursor, entry, mtr);
		if(*rec == NULL){ /*重组后还是失败，打印错误信息*/
			char* err_buf = mem_alloc(1000);

			dtuple_sprintf(err_buf, 900, entry);

			fprintf(stderr, "InnoDB: Error: cannot insert tuple %s to index %s of table %s\n"
				"InnoDB: max insert size %lu\n", err_buf, index->name, index->table->name, max_size);

			mem_free(err_buf);
		}

		ut_a(*rec);
	}

	/*更新HASH索引*/
	if(!reorg && level == 0 && cursor->flag == BTR_CUR_HASH)
		btr_search_update_hash_node_on_insert(cursor);
	else
		btr_search_update_hash_on_insert(cursor);

	/*继承后一行事务锁(GAP方式)*/
	if(!(flags & BTR_NO_LOCKING_FLAG) && inherit)
		lock_update_insert(*rec);

	/*非聚集索引插入*/
	if(!(type & DICT_CLUSTERED))
		ibuf_update_free_bits_if_full(cursor->index, page, max_size,
		rec_size + PAGE_DIR_SLOT_SIZE);

	*big_rec = big_rec_vec;

	return DB_SUCCESS;
}

/*以悲观执行记录的插入,悲观方式是表空间不够，需要扩大表空间*/
ulint btr_cur_pessimistic_insert(ulint flags, btr_cur_t* cursor, dtuple_t* entry, rec_t** rec, big_rec_t** big_rec, que_thr_t* thr, mtr_t* mtr)
{
	dict_index_t*	index = cursor->index;
	big_rec_t*	big_rec_vec	= NULL;
	page_t*		page;
	ulint		err;
	ibool		dummy_inh;
	ibool		success;
	ulint		n_extents = 0;

	*big_rec = NULL;
	page = btr_cur_get_page(cursor);

	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(btr_cur_get_tree(cursor)), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	cursor->flag = BTR_CUR_BINARY;

	err = btr_cur_optimistic_insert(flags, cursor, entry, rec, big_rec, thr, mtr);
	if(err != DB_FAIL) /*乐观锁方式插入成功，直接返回*/
		return err;

	err = btr_cur_ins_lock_and_undo(flags, cursor, entry, thr, &dummy_inh);
	if(err != DB_SUCCESS)
		return err;

	if(!(flags && BTR_NO_UNDO_LOG_FLAG)){
		n_extents = cursor->tree_height / 16 + 3;

		/*扩大file space表空间*/
		success = fsp_reserve_free_extents(index->space, n_extents, FSP_NORMAL, mtr);
		if(!success){ /*超出表空间范围*/
			err =  DB_OUT_OF_FILE_SPACE;
			return err;
		}
	}

	/*单个页无法存储entry记录,作为大记录存储*/
	if(rec_get_converted_size(entry) >= page_get_free_space_of_empty() / 2 || rec_get_converted_size(entry) >= REC_MAX_DATA_SIZE){
		 big_rec_vec = dtuple_convert_big_rec(index, entry, NULL, 0);
		 if(big_rec_vec == NULL)
			 return DB_TOO_BIG_RECORD;
	}

	if (dict_tree_get_page(index->tree) == buf_frame_get_page_no(page))
			*rec = btr_root_raise_and_insert(cursor, entry, mtr);
	else
		*rec = btr_page_split_and_insert(cursor, entry, mtr);
	
	btr_cur_position(index, page_rec_get_prev(*rec), cursor);

	/*更新自适应HASH*/
	btr_search_update_hash_on_insert(cursor);

	/*新插入行对gap行锁的继承*/
	if(!(flags & BTR_NO_LOCKING_FLAG))
		lock_update_insert(*rec);

	err = DB_SUCCESS;

	if(n_extents > 0)
		fil_space_release_free_extents(index->space, n_extents);

	*big_rec = big_rec_vec;

	return err;
}

/*按聚集索引修改记录，进行事务锁请求*/
UNIV_INLINE ulint btr_cur_upd_lock_and_undo(ulint flags, btr_cur_t* cursor, upd_t* update, ulint cmpl_info, que_thr_t* thr, dulint roll_ptr)
{
	dict_index_t*	index;
	rec_t*			rec;
	ulint			err;

	ut_ad(cursor && update && thr && roll_ptr);
	ut_ad(cursor->index->type & DICT_CLUSTERED);

	rec = btr_cur_get_rec(cursor);
	index = cursor->index;

	err =DB_SUCCESS;

	if(!(flags & BTR_NO_LOCKING_FLAG)){
		/*检查修改记录时，记录聚集索引上是否有锁（包括显式锁和隐式锁）*/
		err = lock_clust_rec_modify_check_and_lock(flags, rec, index, thr);
		if(err != DB_SUCCESS)
			return err;
	}

	err = trx_undo_report_row_operation(flags, TRX_UNDO_MODIFY_OP, thr, index, NULL, update,
		cmpl_info, rec, roll_ptr);

	return err;
}

/*为update record增加一条redo log*/
UNIV_INLINE void btr_cur_update_in_place_log(ulint flags, rec_t* rec, dict_index_t* index, 
	upd_t* update, trx_t* trx, dulint roll_ptr, mtr_t* mtr)
{
	byte* log_ptr;

	log_ptr = mlog_open(mtr, 30 + MLOG_BUF_MARGIN);
	log_ptr = mlog_write_initial_log_record_fast(rec, MLOG_REC_UPDATE_IN_PLACE, log_ptr, mtr);
	
	mach_write_to_1(log_ptr, flags);
	log_ptr++;
	
	log_ptr = row_upd_write_sys_vals_to_log(index, trx, roll_ptr, log_ptr, mtr);

	mach_write_to_2(log_ptr, rec - buf_frame_align(rec));
	log_ptr += 2;

	row_upd_index_write_log(update, log_ptr, mtr);
}

/*解析一条修改记录的redo log并进行重演*/
byte* btr_cur_parse_update_in_place(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	ulint	flags;
	rec_t*	rec;
	upd_t*	update;
	ulint	pos;
	dulint	trx_id;
	dulint	roll_ptr;
	ulint	rec_offset;
	mem_heap_t* heap;

	if(end_ptr < ptr + 1)
		return NULL;

	flags = mach_read_from_1(ptr);
	ptr++;

	ptr = row_upd_parse_sys_vals(ptr, end_ptr, &pos, &trx_id, &roll_ptr);

	if(ptr == NULL)
		return NULL;

	if(end_ptr < ptr + 2)
		return NULL;

	/*从redo log中读取修改记录的偏移*/
	rec_offset = mach_read_from_2(ptr);
	ptr += 2;

	/*从redo log中读取修改的内容*/
	heap = mem_heap_create(256);
	ptr = row_upd_index_parse(ptr, end_ptr, heap, &update);
	if(ptr == NULL){
		mem_heap_free(heap);
		return NULL;
	}

	if(page == NULL){
		mem_heap_free(heap);
		return NULL;
	}

	rec = page + rec_offset;
	if(!(flags & BTR_KEEP_SYS_FLAG))
		row_upd_rec_sys_fields_in_recovery(rec, pos, trx_id, roll_ptr);

	/*进行记录修改*/
	row_upd_rec_in_place(heap, update);

	mem_heap_free(heap);

	return ptr;
}
/*通过二级索引修改对应的记录*/
ulint btr_cur_update_sec_rec_in_place(btr_cur_t* cursor, upd_t* update, que_thr_t* thr, mtr_t* mtr)
{
	dict_index_t*	index = cursor->index;
	dict_index_t*	clust_index;
	ulint		err;
	rec_t*		rec;
	dulint		roll_ptr = ut_dulint_zero;
	trx_t*		trx	= thr_get_trx(thr);

	ut_ad(0 == (index->type & DICT_CLUSTERED));

	rec = btr_cur_get_rec(cursor);

	if(btr_cur_print_record_ops && thr){
		printf("Trx with id %lu %lu going to update table %s index %s\n",
			ut_dulint_get_high(thr_get_trx(thr)->id),ut_dulint_get_low(thr_get_trx(thr)->id), index->table_name, index->name);

		rec_print(rec);
	}

	/*通过二级索引获得行上的锁*/
	err = lock_sec_rec_modify_check_and_lock(0, rec, index, thr);
	if(err != DB_SUCCESS)
		return err;

	/*删除自适应HASH对应关系*/
	btr_search_update_hash_on_delete(cursor);
	/*对记录进行修改*/
	row_upd_rec_in_place(rec, update);

	/*通过二级索引找到聚集索引*/
	clust_index = dict_table_get_first_index(index->table);
	/*通过聚集索引添加一条修改记录的redo log*/
	btr_cur_update_in_place_log(BTR_KEEP_SYS_FLAG, rec, clust_index, update, trx, roll_ptr, mtr);
	
	return DB_SUCCESS;
}

ulint btr_cur_update_in_place(ulint flags, btr_cur_t* cursor, upd_t* update, ulint cmpl_info, que_thr_t* thr, mtr_t* mtr)
{
	dict_index_t*	index;
	buf_block_t*	block;
	ulint		err;
	rec_t*		rec;
	dulint		roll_ptr;
	trx_t*		trx;
	ibool		was_delete_marked;

	ut_ad(cursor->index->type & DICT_CLUSTERED);

	/*获得对应的记录、索引、事务*/
	rec = btr_cur_get_rec(cursor);
	index = cursor->index;
	trx = thr_get_trx(thr);

	if(btr_cur_print_record_ops && thr){
		printf("Trx with id %lu %lu going to update table %s index %s\n",
			ut_dulint_get_high(thr_get_trx(thr)->id),
			ut_dulint_get_low(thr_get_trx(thr)->id),
			index->table_name, index->name);

		rec_print(rec);
	}

	/*获得事务的行锁*/
	err = btr_cur_upd_lock_and_undo(flags, cursor, update, cmpl_info, thr, &roll_ptr);
	if(err != DB_SUCCESS)
		return err;

	block = buf_block_align(rec);
	if(block->is_hashed){
		if (row_upd_changes_ord_field_binary(NULL, index, update)) 
			btr_search_update_hash_on_delete(cursor);

		rw_lock_x_lock(&btr_search_latch);
	}

	if(!(flags & BTR_KEEP_SYS_FLAG))
		row_upd_rec_sys_fields(rec, index, trx, roll_ptr);

	was_delete_marked = rec_get_deleted_flag(rec);
	/*进行记录更新*/
	row_upd_rec_in_place(rec, update);

	if(block->is_hashed)
		rw_lock_x_unlock(&btr_search_latch);

	btr_cur_update_in_place_log(flags, rec, index, update, trx, roll_ptr, mtr);

	/*由删除状态变成未删除状态*/
	if(was_delete_marked && !rec_get_deleted_flag(rec))
		btr_cur_unmark_extern_fields(rec, mtr);

	return DB_SUCCESS;
}

/*乐观方式更新一条记录，先尝试在原来的记录位置直接更新，如果不能就会将原来的记录删除，将更新的内容组成一条记录插入到对应的page中*/
ulint btr_cur_optimistic_update(ulint flags, btr_cur_t* cursor, upd_t* update, ulint cmpl_info, que_thr_t* thr, mtr_t* mtr)
{
	dict_index_t*	index;
	page_cur_t*	page_cursor;
	ulint		err;
	page_t*		page;
	rec_t*		rec;
	ulint		max_size;
	ulint		new_rec_size;
	ulint		old_rec_size;
	dtuple_t*	new_entry;
	dulint		roll_ptr;
	trx_t*		trx;
	mem_heap_t*	heap;
	ibool		reorganized	= FALSE;
	ulint		i;

	ut_ad((cursor->index)->type & DICT_CLUSTERED);

	page = btr_cur_get_page(cursor);
	rec = btr_cur_get_rec(cursor);
	index = cursor->index;

	if (btr_cur_print_record_ops && thr) {
		printf("Trx with id %lu %lu going to update table %s index %s\n",
			ut_dulint_get_high(thr_get_trx(thr)->id),
			ut_dulint_get_low(thr_get_trx(thr)->id),
			index->table_name, index->name);

		rec_print(rec);
	}

	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_BUF_FIX));

	/*不能改变列的大小，直接在原来的记录位置做更新*/
	if(!row_upd_changes_field_size(rec, index, update))
		return btr_cur_update_in_place(flags, cursor, update, cmpl_info, thr, mtr);

	/*判断列是否溢出*/
	for(i = 0; i < upd_get_n_fields(update); i++){
		if(upd_get_nth_field(update, i)->extern_storage)
			return DB_OVERFLOW;
	}

	/*列已是两个字节表示长度*/
	if(rec_contains_externally_stored_field(btr_cur_get_rec(cursor)))
		return DB_OVERFLOW;

	page_cursor = btr_cur_get_page_cur(cursor);
	heap = mem_heap_create(1024);

	/*在内存中构建一个新记录的存储对象记录*/
	new_entry = row_rec_to_index_entry(ROW_COPY_DATA, index, rec, heap);
	row_upd_clust_index_replace_new_col_vals(new_entry, update);

	old_rec_size = rec_get_size(rec);
	new_rec_size = rec_get_converted_size(new_entry);

	/*跟新的记录大小已经超过页能存储的大小*/
	if(new_rec_size >= page_get_free_space_of_empty() / 2){
		mem_heap_free(heap);
		return DB_OVERFLOW;
	}
	/*计算页重组后可以存储的最大空间*/
	max_size = old_rec_size + page_get_max_insert_size_after_reorganize(page, 1);
	if(page_get_data_size(page) - old_rec_size + new_rec_size < BTR_CUR_PAGE_COMPRESS_LIMIT){
		/*记录更新后会触发btree的合并填充，不做直接更新*/
		mem_heap_free(heap);
		return DB_UNDERFLOW;
	}

	/*能存储的空间小于合并的空间阈值或者不能存下新的记录,并且page的空间不只1条记录*/
	if(!((max_size >= BTR_CUR_PAGE_REORGANIZE_LIMIT && max_size >= new_rec_size) || page_get_n_recs(page) <= 1)){
		mem_heap_free(heap);
		return DB_OVERFLOW;
	}

	/*不能对记录加事务锁*/
	err = btr_cur_upd_lock_and_undo(flags, cursor, update, cmpl_info, thr, &roll_ptr);
	if(err != DB_SUCCESS){
		mem_heap_free(heap);
		return err;
	}

	/*将记录的行锁全部转移到infimum,应该是临时存储在这个地方*/
	lock_rec_store_on_page_infimum(rec);

	btr_search_update_hash_on_delete(cursor);
	/*将page游标对应的记录删除*/
	page_cur_delete_rec(page_cursor, mtr);
	/*游标移到前面一条记录上*/
	page_cur_move_to_prev(page_cursor);

	trx = thr_get_trx(thr);

	if(!(flags & BTR_KEEP_SYS_FLAG)){
		row_upd_index_entry_sys_field(new_entry, index, DATA_ROLL_PTR, roll_ptr);
		row_upd_index_entry_sys_field(new_entry, index, DATA_TRX_ID, trx->id);
	}
	/*将新构建的tuple记录插入到页中*/
	rec = btr_cur_insert_if_possible(cursor, new_entry, &reorganized, mtr);
	ut_a(rec);

	if(!rec_get_deleted_flag(rec))
		btr_cur_unmark_extern_fields(rec, mtr);

	/*将缓存在infimum上的锁恢复到新插入的记录上*/
	lock_rec_restore_from_page_infimum(rec, page);

	page_cur_move_to_next(page_cursor);

	mem_heap_free(heap);

	return DB_SUCCESS;
}

static void btr_cur_pess_upd_restore_supremum(rec_t* rec, mtr_t* mtr)
{
	page_t*	page;
	page_t*	prev_page;
	ulint	space;
	ulint	prev_page_no;

	page = buf_frame_align(rec);
	if(page_rec_get_next(page_get_infimum_rec(page) != rec))
		return;

	/*获得rec前一页的page对象*/
	space = buf_frame_get_space_id(page);
	prev_page_no = btr_page_get_prev(page, mtr);

	ut_ad(prev_page_no != FIL_NULL);
	prev_page = buf_page_get_with_no_latch(space, prev_page_no, mtr);

	/*确定已经拥有x-latch*/
	ut_ad(mtr_memo_contains(mtr, buf_block_align(prev_page), MTR_MEMO_PAGE_X_FIX));
	/*前一个page的supremum继承rec上的锁*/
	lock_rec_reset_and_inherit_gap_locks(page_get_supremum_rec(prev_page), rec);
}

/*将update中的修改列数据更新到tuple逻辑记录当中*/
static void btr_cur_copy_new_col_vals(dtuple_t* entry, upd_t* update, mem_heap_t* heap)
{
	upd_field_t*	upd_field;
	dfield_t*	dfield;
	dfield_t*	new_val;
	ulint		field_no;
	byte*		data;
	ulint		i;

	dtuple_set_info_bits(entry, update->info_bits);

	/*将update中所有更改的列数据依次替换到dtuple对应的列中*/
	for(i = 0; i < upd_get_n_fields(update); i ++){
		upd_field = upd_get_nth_field(update, i);
		field_no = upd_field->field_no;
		dfield = dtuple_get_nth_field(entry, field_no);

		new_val = &(upd_field->new_val);
		if(new_val->len = UNIV_SQL_NULL)
			data = NULL;
		else{
			data = mem_heap_alloc(heap, new_val->len);
			ut_memcpy(data, new_val->data, new_val->len);
		}

		/*此处为0拷贝*/
		dfield_set_data(dfield, data, new_val->len);
	}
}

/*悲观式更新记录*/
ulint btr_cur_pessimistic_update(ulint flags, btr_cur_t* cursor, big_rec_t** big_rec, upd_t* update, ulint cmpl_info, que_thr_t* thr, mtr_t* mtr)
{
	big_rec_t*	big_rec_vec	= NULL;
	big_rec_t*	dummy_big_rec;
	dict_index_t*	index;
	page_t*		page;
	dict_tree_t*	tree;
	rec_t*		rec;
	page_cur_t*	page_cursor;
	dtuple_t*	new_entry;
	mem_heap_t*	heap;
	ulint		err;
	ulint		optim_err;
	ibool		dummy_reorganized;
	dulint		roll_ptr;
	trx_t*		trx;
	ibool		was_first;
	ibool		success;
	ulint		n_extents	= 0;
	ulint*		ext_vect;
	ulint		n_ext_vect;
	ulint		reserve_flag;

	*big_rec = NULL;

	page = btr_cur_get_page(cursor);
	rec = btr_cur_get_rec(cursor);
	index = cursor->index;
	tree = index->tree;

	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	/*先尝试乐观式的更新*/
	optim_err = btr_cur_optimistic_update(flags, cursor, update, cmpl_info, thr, mtr);
	if(optim_err != DB_UNDERFLOW && optim_err != DB_OVERFLOW)
		return optim_err;

	err = btr_cur_upd_lock_and_undo(flags, cursor, update, cmpl_info, thr, &roll_ptr);
	if(err != DB_SUCCESS)
		return err;

	if(optim_err == DB_OVERFLOW){
		n_extents = cursor->tree_height / 16 + 3;
		if(flags & BTR_NO_UNDO_LOG_FLAG)
			reserve_flag = FSP_CLEANING;
		else
			reserve_flag = FSP_NORMAL;

		/*尝试扩大表空间*/
		success = fsp_reserve_free_extents(cursor->index->space, n_extents, reserve_flag, mtr);
		if(!success)
			err = DB_OUT_OF_FILE_SPACE;
		return err;
	}
	
	heap = mem_heap_create(1024);
	trx = thr_get_trx(thr);
	new_entry = row_rec_to_index_entry(ROW_COPY_DATA, index, rec, heap);

	/*将更新列数据同步到new_entry中*/
	btr_cur_copy_new_col_vals(new_entry, update, heap);

	/*记录锁转到infimum上临时保存*/
	lock_rec_store_on_page_infimum(rec);

	btr_search_update_hash_on_delete(cursor);

	if(flags & BTR_NO_UNDO_LOG_FLAG){
		ut_a(big_rec_vec == NULL);
		btr_rec_free_updated_extern_fields(index, rec, update, TRUE, mtr);
	}

	ext_vect = mem_heap_alloc(heap, Size(ulint) * rec_get_n_fields(rec));
	n_ext_vect = btr_push_update_extern_fields(ext_vect, rec, update);

	/*删除游标处的记录*/
	page_cur_delete_rec(page_cursor, mtr);
	page_cur_move_to_prev(page_cursor);

	/*是个大记录,进行big_rec转换*/
	if((rec_get_converted_size(new_entry) >= page_get_free_space_of_empty() / 2)
		|| (rec_get_converted_size(new_entry) >= REC_MAX_DATA_SIZE)) {
			big_rec_vec = dtuple_convert_big_rec(index, new_entry, ext_vect, n_ext_vect);
			if(big_rec_vec == NULL){
				mem_heap_free(heap);
				goto return_after_reservations;
			}
	}

	/*将tuple插入到page中*/
	rec = btr_cur_insert_if_possible(cursor, new_entry, &dummy_reorganized, mtr);
	if(rec != NULL){
		/*插入成功，将寄存在infimum中的行锁转移回来*/
		lock_rec_restore_from_page_infimum(rec, page);
		rec_set_field_extern_bits(rec, ext_vect, n_ext_vect, mtr);
		
		if(!rec_get_deleted_flag(rec))
			btr_cur_unmark_extern_fields(rec, mtr);

		/*TODO:??*/
		btr_cur_compress_if_useful(cursor, mtr);

		err = DB_SUCCESS;
		mem_heap_free(heap);

		goto return_after_reservations;
	}

	/*判断游标是不是指向最开始的记录infimum*/
	if(page_cur_is_before_first(page_cursor))
		was_first = TRUE;
	else
		was_first = FALSE;

	/*尝试用乐观式插入tuple,这个动作有可能照成*/
	err = btr_cur_pessimistic_insert(BTR_NO_UNDO_LOG_FLAG
		| BTR_NO_LOCKING_FLAG
		| BTR_KEEP_SYS_FLAG, cursor, new_entry, &rec, &dummy_big_rec, NULL, mtr);
	ut_a(rec);
	ut_a(err == DB_SUCCESS);
	ut_a(dummy_big_rec == NULL);

	rec_set_field_extern_bits(rec, ext_vect, n_ext_vect, mtr);

	if(!rec_get_deleted_flag(rec))
		btr_cur_unmark_extern_fields(rec, mtr);

	lock_rec_restore_from_page_infimum(rec, page);

	/*如果没有infimum，那么行锁可能全部转移到了前一页的supremum上,尤其是在分裂的时候可能产生这个*/
	if(!was_first)
		btr_cur_pess_upd_restore_supremum(rec, mtr);

	mem_heap_free(heap);

return_after_reservations:
	if(n_extents > 0) /*记录只是插入在ibuffer中，并没有刷到磁盘，所以会先标记为未占用状态*/
		fil_space_release_free_extents(cursor->index->space, n_extents);

	*big_rec = big_rec_vec;

	return err;
}

/*通过聚集索引删除记录的redo log*/
UNIV_INLINE void btr_cur_del_mark_set_clust_rec_log(ulint flags, rec_t* rec, dict_index_t* index, ibool val, 
					trx_t* trx, dulint roll_ptr, mtr_t* mtr)
{
	byte* log_ptr;
	log_ptr = mlog_open(mtr, 30);
	/*构建一条CLUST DELETE MARK的redo log*/
	log_ptr = mlog_write_initial_log_record_fast(rec, MLOG_REC_CLUST_DELETE_MARK, log_ptr, mtr); 
	mach_write_to_1(log_ptr, flags);
	log_ptr ++;
	mach_write_to_1(log_ptr, val);
	log_ptr ++;

	/*将事务的ID和回滚位置写入到redo log中*/
	log_ptr = row_upd_write_sys_vals_to_log(index, trx, roll_ptr, log_ptr, mtr);
	mach_write_to_2(log_ptr, rec - buf_frame_align(rec));
	log_ptr += 2;

	mlog_close(mtr, log_ptr);
}

/*redo 过程对CLUST DELETE MARK日志的重演*/
byte* btr_cur_parse_del_mark_set_clust_rec(byte* ptr, byte* end_ptr, page_t* page)
{
	ulint	flags;
	ibool	val;
	ulint	pos;
	dulint	trx_id;
	dulint	roll_ptr;
	ulint	offset;
	rec_t*	rec;

	if(end_ptr < ptr + 2)
		return NULL;

	flags = mach_read_from_1(ptr);
	ptr++;
	val = mach_read_from_1(ptr);
	ptr++;

	ptr = row_upd_parse_sys_vals(ptr, end_ptr, &pos, &trx_id, &roll_ptr);
	if(ptr == NULL)
		return NULL;

	if(end_ptr < ptr + 2)
		return NULL;

	/*获得记录的偏移,可通过这个偏移得到记录的位置*/
	offset = mach_read_from_2(ptr);
	ptr += 2;

	if(page != NULL){
		rec = page + offset;
		if(!(flags & BTR_KEEP_SYS_FLAG))
			row_upd_rec_sys_fields_in_recovery(rec, pos, trx_id, roll_ptr);

		/*将记录设置为删除状态*/
		rec_set_deleted_flag(rec, val);
	}

	return ptr;
}

ulint btr_cur_del_mark_set_clust_rec(ulint flags, btr_cur_t* cursor, ibool val, que_thr_t* thr, mtr_t* mtr)
{
	dict_index_t*	index;
	buf_block_t*	block;
	dulint		roll_ptr;
	ulint		err;
	rec_t*		rec;
	trx_t*		trx;

	rec = btr_cur_get_rec(cursor);
	index = cursor->index;

	if(btr_cur_print_record_ops && thr){
		printf("Trx with id %lu %lu going to del mark table %s index %s\n",
			ut_dulint_get_high(thr_get_trx(thr)->id),
			ut_dulint_get_low(thr_get_trx(thr)->id),
			index->table_name, index->name);

		rec_print(rec);
	}

	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(rec_get_deleted_flag(rec) == FALSE);

	/*尝试通过聚集索引获得rec记录行的锁执行权，如果有隐士锁，转换成显示锁*/
	err = lock_clust_rec_modify_check_and_lock(flags, rec, index, thr);
	if(err != DB_SUCCESS)
		return err;

	/*获得事务锁权后,添加undo log日志*/
	err = trx_undo_report_row_operation(flags, TRX_UNDO_MODIFY_OP, thr,
		index, NULL, NULL, 0, rec, &roll_ptr);
	if(err != DB_SUCCESS)
		return err;

	block = buf_block_align(rec);

	if(block->is_hashed)
		rw_lock_x_lock(&btr_search_latch);

	/*对记录做删除标识*/
	rec_set_deleted_flag(rec, val);

	trx = thr_get_trx(thr);
	if(!(flags & BTR_KEEP_SYS_FLAG)){
		row_upd_rec_sys_fields(&btr_search_latch);
	}

	if(block->is_hashed)
		rw_lock_x_unlock(&btr_search_latch);

	/*记录redo log日志*/
	btr_cur_del_mark_set_clust_rec_log(flags, rec, index, val, trx, roll_ptr, mtr);

	return DB_SUCCESS;
}

/*写入通过二级索引删除记录的redo log*/
UNIV_INLINE void btr_cur_del_mark_set_sec_rec_log(rec_t* rec, ibool val, mtr_t* mtr)
{
	byte* log_ptr;

	log_ptr = mlog_open(mtr, 30);
	/*加入一条SEC DELETE MARK删除记录的redo log*/
	log_ptr =  mlog_write_initial_log_record_fast(rec, MLOG_REC_SEC_DELETE_MARK, log_ptr, mtr);
	mach_write_to_1(log_ptr, val);
	log_ptr ++;

	mach_write_to_2(log_ptr, rec - buf_frame_align(rec));
	log_ptr += 2;

	mlog_close(mtr, log_ptr);
}

/*重演SEC DELETE MARK日志*/
byte* btr_cur_parse_del_mark_set_sec_rec(byte* ptr, byte* end_ptr, page_t* page)
{
	ibool	val;
	ulint	offset;
	rec_t*	rec;

	if(end_ptr < ptr + 3)
		return NULL;

	val = mach_read_from_1(ptr);
	ptr ++;

	offset = mach_read_from_2(ptr);
	ptr += 2;

	ut_a(offset <= UNIV_PAGE_SIZE);
	if(page){
		rec = page + offset;
		rec_set_deleted_flag(rec, val);
	}

	return ptr;
}

/*通过二级索引删除记录*/
ulint btr_cur_del_mark_set_sec_rec(ulint flags, btr_cur_t* cursor, ibool val, que_thr_t* thr, mtr_t* mtr)
{
	buf_block_t*	block;
	rec_t*		rec;
	ulint		err;

	rec = btr_cur_get_rec(cursor);

	if (btr_cur_print_record_ops && thr) {
		printf("Trx with id %lu %lu going to del mark table %s index %s\n",
			ut_dulint_get_high(thr_get_trx(thr)->id),
			ut_dulint_get_low(thr_get_trx(thr)->id),
			cursor->index->table_name, cursor->index->name);
		rec_print(rec);
	}

	err = lock_sec_rec_modify_check_and_lock(flags, rec, cursor->index, thr);
	if(err != DB_SUCCESS)
		return err;

	block = buf_block_align(rec);

	if(block->is_hashed)
		rw_lock_x_lock(&btr_search_latch);

	rec_set_deleted_flag(rec, val);

	if(block->is_hashed)
		rw_lock_x_unlock(&btr_search_latch);

	btr_cur_del_mark_set_sec_rec_log(rec, val, mtr);

	return DB_SUCCESS;
}

/*直接从IBUF中删除*/
void btr_cur_del_unmark_for_ibuf(rec_t* rec, mtr_t* mtr)
{
	rec_set_deleted_flag(rec, FALSE);
	btr_cur_del_mark_set_sec_rec_log(rec, FALSE, mtr);
}

/*尝试压缩合并一个btree上的叶子节点page*/
void btr_cur_compress(btr_cur_t* cursor, mtr_t* mtr)
{
	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(btr_cur_get_tree(cursor)), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(btr_cur_get_page(cursor)), MTR_MEMO_PAGE_X_FIX));
	ut_ad(btr_page_get_level(btr_cur_get_page(cursor), mtr) == 0);

	btr_compress(cursor, mtr);
}

/*判断是否可以进行page compress,如果能，进行btree page compress*/
ibool btr_cur_compress_if_useful(btr_cur_t* cursor, mtr_t* mtr)
{
	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(btr_cur_get_tree(cursor)), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(mtr, buf_block_align( btr_cur_get_page(cursor)), MTR_MEMO_PAGE_X_FIX));
	/*根据填充因子判断是否需要对cursor指向的叶子节点*/
	if(btr_cur_compress_recommendation(cursor, mtr)){
		btr_compress(cursor, mtr);
		return TRUE;
	}

	return FALSE;
}

/*乐观式删除，不涉及IO操作*/
ibool btr_cur_optimistic_delete(btr_cur_t* cursor, mtr_t* mtr)
{
	page_t*	page;
	ulint	max_ins_size;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(btr_cur_get_page(cursor)), MTR_MEMO_PAGE_X_FIX));

	page = btr_cur_get_page(cursor);
	ut_ad(btr_page_get_level(page, mtr) == 0);

	/*如果一个列被分在多个页中存储的话，暂时不做删除，因为涉及到多页合并会触发IO操作*/
	if(rec_contains_externally_stored_field(btr_cur_get_rec(cursor)))
		return FALSE;

	/*删除cursor不会触发page的compress*/
	if(btr_cur_can_delete_without_compress(cursor, mtr)){
		/*记录删除后，行锁会转移到后面一行上*/
		lock_update_delete(btr_cur_get_rec(cursor));
		btr_search_update_hash_on_delete(cursor);

		max_ins_size = page_get_max_insert_size_after_reorganize(page, 1);
		/*记录删除*/
		page_cur_delete_rec(btr_cur_get_page_cur(cursor), mtr);
		/*更新ibuf对应的page状态*/
		ibuf_update_free_bits_low(cursor->index, page, max_ins_size, mtr);
		
		return TRUE;
	}

	return FALSE;
}

/*悲观式删除,涉及表空间的改变*/
ibool btr_cur_pessimistic_delete(ulint* err, ibool has_reserved_extents, btr_cur_t* cursor, ibool in_roolback, mtr_t* mtr)
{
	page_t*		page;
	dict_tree_t*	tree;
	rec_t*		rec;
	dtuple_t*	node_ptr;
	ulint		n_extents	= 0;
	ibool		success;
	ibool		ret		= FALSE;
	mem_heap_t*	heap;

	page = btr_cur_get_page(cursor);
	tree = btr_cur_get_tree(cursor);

	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	if(!has_reserved_extents){
		n_extents = cursor->tree_height / 32 + 1;
		success = fsp_reserve_free_extents(cursor->index->space, n_extents, FSP_CLEANING, mtr);
		if(!success){
			*err = DB_OUT_OF_FILE_SPACE;
			return FALSE;
		}
	}

	/*是否列夸页存储的页*/
	btr_rec_free_externally_stored_fields(cursor->index, btr_cur_get_rec(cursor), in_roolback, mtr);

	/*page中只有1条记录，并且cursor索引指向的页不是cursor当前指向的page,有可能是列太大分开存储占用的页,page进行废弃*/
	if(page_get_n_recs(page) < 2 && dict_tree_get_page(btr_cur_get_tree(cursor)) != buf_frame_get_page_no(page)){
		btr_discard_page(cursor, mtr);
		*err = DB_SUCCESS;
		ret = TRUE;

		goto return_after_reservations;
	}

	rec = btr_cur_get_rec(cursor);
	/*行锁转移到rec的下一条记录*/
	lock_update_delete(rec);

	/*page不是处于叶子节点上，而且处于第一条记录上*/
	if(btr_page_get_level(page, mtr) > 0 && page_rec_get_next(page_get_infimum_rec(page)) == rec){
		if(btr_page_get_prev(page, mtr) == FIL_NULL) /*前面没有兄弟页，那么第一条记录必须为min rec*/
			btr_set_min_rec_mark(page_rec_get_next(rec), mtr);
		else{
			/*先将page的node ptr从父亲节点上删除*/
			btr_node_ptr_delete(tree, page, mtr);

			heap = mem_heap_create(256);
			/*将rec的下一条记录的key作为node ptr里的新的值插入到父亲节点上*/
			node_ptr = dict_tree_build_node_ptr(tree, page_rec_get_next(rec), buf_frame_get_page_no(page), heap, btr_page_get_level(page, mtr));
			btr_insert_on_non_leaf_level(tree, btr_page_get_level(page, mtr) + 1, node_ptr, mtr);

			mem_heap_free(heap);
		}
	}

	btr_search_update_hash_on_delete(cursor);
	/*删除记录*/
	page_cur_delete_rec(btr_cur_get_page_cur(cursor), mtr);

	ut_ad(btr_check_node_ptr(tree, page, mtr));
	*err = DB_SUCCESS;

return_after_reservations:
	if(!ret) /*进行合并判断,如果条件满足就会触发合并*/
		ret = btr_cur_compress_if_useful(cursor, mtr);

	if(n_extents > 0)
		fil_space_release_free_extents(cursor->index->space, n_extents);

	return ret;
}

static void btr_cur_add_path_info(btr_cur_t* cursor, ulint height, ulint root_height)
{
	btr_path_t*	slot;
	rec_t*		rec;

	ut_a(cursor->path_arr);

	if(root_height >= BTR_PATH_ARRAY_N_SLOTS - 1){ /*root 的层高*/
		slot = cursor->path_arr;
		slot->nth_rec = ULINT_UNDEFINED;
		return ;
	}

	if(height == 0){  
		slot = cursor->path_arr + root_height + 1;
		slot->nth_rec = ULINT_UNDEFINED;
	}

	rec = btr_cur_get_rec(cursor);
	slot = cursor->path_arr + (root_height - height);

	slot->nth_rec = page_rec_get_n_recs_before(rec);
	slot->n_recs = page_get_n_recs(buf_frame_align(rec));
}

/*估算tuple1 tuple2之间的记录行数，只是近似值，不是精确值*/
ib_longlong btr_estimate_n_rows_in_range(dict_index_t* index, dtuple_t* tuple1, ulint mode1, dtuple_t* tuple2, ulint mode2)
{
	btr_path_t	path1[BTR_PATH_ARRAY_N_SLOTS];
	btr_path_t	path2[BTR_PATH_ARRAY_N_SLOTS];
	btr_cur_t	cursor;
	btr_path_t*	slot1;
	btr_path_t*	slot2;
	ibool		diverged;
	ibool       diverged_lot;
	ulint       divergence_level;           
	ib_longlong	n_rows;
	ulint		i;
	mtr_t		mtr;

	mtr_start(&mtr);

	cursor.path_arr = path1;
	if(dtuple_get_n_fields(tuple1) > 0){
		/*将cursor定位到与tuple1索引相同的page记录上,并将*/
		btr_cur_search_to_nth_level(index, 0, tuple1, mode1, BTR_SEARCH_LEAF | BTR_ESTIMATE, &cursor, 0, &mtr);
	}
	else{
		btr_cur_open_at_index_side(TRUE, index, BTR_SEARCH_LEAF | BTR_ESTIMATE, &cursor, &mtr);
	}

	mtr_commit(&mtr);

	mtr_start(&mtr);

	cursor.path_arr = path2;
	if(dtuple_get_n_fields(tuple2) > 0){
		btr_cur_search_to_nth_level(index, 0, tuple2, mode2, BTR_SEARCH_LEAF | BTR_ESTIMATE, &cursor, 0, &mtr);
	}
	else{
		btr_cur_open_at_index_side(FALSE, index, BTR_SEARCH_LEAF | BTR_ESTIMATE, &cursor, &mtr);
	}

	mtr_commit(&mtr);

	n_rows = 1;
	diverged = FALSE;
	diverged_lot = FALSE;

	divergence_level = 1000000;

	for(i = 0; ; i ++){
		ut_ad(i < BTR_PATH_ARRAY_N_SLOTS);

		slot1 = path1 + i;
		slot2 = path2 + i;

		/*已经计算到叶子节点了*/
		if(slot1->nth_rec == ULINT_UNDEFINED || slot2->nth_rec == ULINT_UNDEFINED){
			if(i > divergence_level + 1)
				n_rows = n_rows * 2;

			if(n_rows > index->table->stat_n_rows / 2){
				n_rows = index->table->stat_n_rows / 2;
				if(n_rows == 0)
					n_rows = index->table->stat_n_rows;
			}

			return n_rows;
		}

		if(!diverged && slot1->nth_rec != slot2->nth_rec){
			diverged = TRUE;
			if(slot1->nth_rec < slot2->nth_rec){ /*计算root page上的相隔的页数*/
				n_rows = slot2->nth_rec - slot1->nth_rec;
				if(n_rows > 1){
					diverged_lot = TRUE;
					divergence_level = i;
				}
			}
			else /*如果tuple2在tuple1前面，那么返回一个象征性的值10*/
				return 10;
		}
		else if(diverged && !diverged_lot){ /*在同一页中,只统计差距*/
			if (slot1->nth_rec < slot1->n_recs || slot2->nth_rec > 1) {

				diverged_lot = TRUE;
				divergence_level = i;

				n_rows = 0;

				if (slot1->nth_rec < slot1->n_recs) {
					n_rows += slot1->n_recs - slot1->nth_rec;
				}

				if (slot2->nth_rec > 1) {
					n_rows += slot2->nth_rec - 1;
				}
			}
		}
		else if(diverged_lot)/*在不同页中，统计相隔页中所有的数据作为评估值*/
			n_rows = (n_rows * (slot1->n_recs + slot2->n_recs)) / 2;
	}
}

/*统计key值不同的个数，就是Cardinality的值*/
void btr_estimate_number_of_different_key_vals(dict_index_t* index)
{
	btr_cur_t	cursor;
	page_t*		page;
	rec_t*		rec;
	ulint		n_cols;
	ulint		matched_fields;
	ulint		matched_bytes;
	ulint*		n_diff;
	ulint		not_empty_flag	= 0;
	ulint		total_external_size = 0;
	ulint		i;
	ulint		j;
	ulint		add_on;
	mtr_t		mtr;

	/*计算索引列的数量*/
	n_cols = dict_index_get_n_unique(index);
	n_diff = mem_alloc((n_cols + 1) * sizeof(ib_longlong));
	for(j = 0; j <= n_cols; j ++)
		n_diff[j] = 0;

	/*进行随机取8个页作为采样,统计不同记录的个数*/
	for(i = 0; i < BTR_KEY_VAL_ESTIMATE_N_PAGES; i ++){
		mtr_start(&mtr);
		btr_cur_open_at_rnd_pos(index, BTR_SEARCH_LEAF, &cursor, &mtr);

		page = btr_cur_get_page(&cursor);
		rec = page_get_infimum_rec(page);
		rec = page_rec_get_next(rec);

		if(rec != page_get_supremum_rec(page))
			not_empty_flag = 1;

		while(rec != page_get_supremum_rec(page) && page_rec_get_next(rec) != page_get_supremum_rec(page)){
			matched_fields = 0;
			matched_bytes = 0;

			cmp_rec_rec_with_match(rec, page_rec_get_next(rec), index, &matched_fields, &matched_bytes);
			for (j = matched_fields + 1; j <= n_cols; j++)
				n_diff[j]++;

			total_external_size += btr_rec_get_externally_stored_len(rec);

			rec = page_rec_get_next(rec);
		}

		total_external_size += btr_rec_get_externally_stored_len(rec);
		mtr_commit(&mtr);
	}
	/*进行平均计算*/
	for(j = 0; j <= n_cols; j ++){
		index->stat_n_diff_key_vals[j] =
			(n_diff[j] * index->stat_n_leaf_pages + BTR_KEY_VAL_ESTIMATE_N_PAGES - 1 + total_external_size + not_empty_flag) / (BTR_KEY_VAL_ESTIMATE_N_PAGES + total_external_size);

		add_on = index->stat_n_leaf_pages / (10 * (BTR_KEY_VAL_ESTIMATE_N_PAGES + total_external_size));

		if(add_on > BTR_KEY_VAL_ESTIMATE_N_PAGES)
			add_on = BTR_KEY_VAL_ESTIMATE_N_PAGES;

		index->stat_n_diff_key_vals[j] += add_on;
	}

	mem_free(n_diff);
}

/*获得rec额外占用的页数*/
static ulint btr_rec_get_externally_stored_len(rec_t* rec)
{
	ulint	n_fields;
	byte*	data;
	ulint	local_len;
	ulint	extern_len;
	ulint	total_extern_len = 0;
	ulint	i;

	if(rec_get_data_size(rec) <= REC_1BYTE_OFFS_LIMIT)
		return 0;

	n_fields = rec_get_n_fields(rec);
	for(i = 0; i < n_fields; i ++){
		data = rec_get_nth_field(rec, i, &local_len);
		local_len -= BTR_EXTERN_FIELD_REF_SIZE;
		extern_len = mach_read_from_4(data + local_len + BTR_EXTERN_LEN + 4); /*或的列的总长度？*/

		total_extern_len += ut_calc_align(extern_len, UNIV_PAGE_SIZE);
	}

	return total_extern_len / UNIV_PAGE_SIZE;
}

/*设置外部存储列的ownership位*/
static void btr_cur_set_ownership_of_extern_field(rec_t* rec, ulint i, ibool val, mtr_t* mtr)
{
	byte*	data;
	ulint	local_len;
	ulint	byte_val;

	data = rec_get_nth_field(rec, i, &local_len);
	ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

	local_len -= BTR_EXTERN_FIELD_REF_SIZE;

	byte_val = mach_read_from_1(data + local_len + BTR_EXTERN_LEN);
	if(val)
		byte_val = byte_val & (~BTR_EXTERN_OWNER_FLAG);
	else
		byte_val = byte_val | BTR_EXTERN_OWNER_FLAG;

	mlog_write_ulint(data + local_len + BTR_EXTERN_LEN, byte_val, MLOG_1BYTE, mtr);
}

void btr_cur_mark_extern_inherited_fields(rec_t* rec, upd_t* update, mtr_t* mtr)
{
	ibool	is_updated;
	ulint	n;
	ulint	j;
	ulint	i;

	n = rec_get_n_fields(rec);

	for(i = 0; i < n; i++){
		if (rec_get_nth_field_extern_bit(rec, i)){
			is_updated = FALSE;

			if(update){
				for(j = 0; j < upd_get_n_fields(update); j ++){
					if(upd_get_nth_field(update, j)->field_no = i)
						is_updated = TRUE;
				}
			}

			/*取消owership位*/
			if(!is_updated)
				btr_cur_set_ownership_of_extern_field(rec, i, FALSE, mtr);
		}
	}
}

void btr_cur_mark_dtuple_inherited_extern(dtuple_t* entry, ulint* ext_vec, ulint n_ext_vec, upd_t* update)
{
	dfield_t* dfield;
	ulint	byte_val;
	byte*	data;
	ulint	len;
	ibool	is_updated;
	ulint	j;
	ulint	i;

	if(ext_vec = NULL)
		return ;

	for(i = 0; i < n_ext_vec; i ++){
		is_updated = FALSE;

		for(j = 0; j < upd_get_n_fields(update); j ++){
			if(upd_get_nth_field(update, j)->field_no == ext_vec[i])
				is_updated = TRUE;
		}

		/*设置extern继承标识*/
		if(!is_updated){
			dfield = dtuple_get_nth_field(entry, ext_vec[i]);
			data = dfield_get_data(dfield);
			len = dfield_get_len(dfield);

			len -= BTR_EXTERN_FIELD_REF_SIZE;

			byte_val = mach_read_from_1(data + len + BTR_EXTERN_LEN);
			byte_val = byte_val | BTR_EXTERN_INHERITED_FLAG;

			mach_write_to_1(data + len + BTR_EXTERN_LEN, byte_val);
		}
	}
}

void btr_cur_unmark_extern_fields(rec_t* rec, mtr_t* mtr)
{
	ulint n, i;

	n = rec_get_n_fields(rec);
	for(i = 0; i < n; i ++){
		if(rec_get_nth_field_extern_bit(rec, i)) /*设置列的ownership位*/
			btr_cur_set_ownership_of_extern_field(rec, i, TRUE, mtr);
	}
}

/*取消tuple对应列的ownership位*/
void btr_cur_unmark_dtuple_extern_fields(dtuple_t* entry, ulint* ext_vec, ulint n_ext_vec)
{
	dfield_t* dfield;
	ulint	byte_val;
	byte*	data;
	ulint	len;
	ulint	i;

	for(i = 0; i < n_ext_vec; i ++){
		dfield = dtuple_get_nth_field(entry, ext_vec[i]);

		data = dfield_get_data(dfield);
		len = dfield_get_len(dfield);

		len -= BTR_EXTERN_FIELD_REF_SIZE;

		byte_val = mach_read_from_1(data + len + BTR_EXTERN_LEN);
		byte_val = byte_val & ~BTR_EXTERN_OWNER_FLAG;

		/*取消ownership位*/
		mach_write_to_1(data + len + BTR_EXTERN_LEN, byte_val);
	}
}

ulint btr_push_update_extern_fields(ulint* ext_vect, rec_t* rec, upd_t* update)
{
	ulint	n_pushed	= 0;
	ibool	is_updated;
	ulint	n;
	ulint	j;
	ulint	i;

	/*计算update中改变的列ID*/
	if(update){
		n = upd_get_n_fields(update);

		for (i = 0; i < n; i++) {
			if (upd_get_nth_field(update, i)->extern_storage){
				ext_vect[n_pushed] =upd_get_nth_field(update, i)->field_no;
				n_pushed++;
			}
		}
	}

	/*计算rec被改变的列ID*/
	n = rec_get_n_fields(rec);
	for(i = 0; i < n; i ++){
		if(rec_get_nth_field_extern_bit(rec, i)){
			is_updated = FALSE;
			if (update) {
				for (j = 0; j < upd_get_n_fields(update); j++) {
						if (upd_get_nth_field(update, j)->field_no == i)
								is_updated = TRUE;
				}
			}

			if(!is_updated){
				ext_vect[n_pushed] = i;
				n_pushed ++;
			}
		}
	}

	return n_pushed;
}

/*获得blob列的长度*/
static ulint btr_blob_get_part_len(byte* blob_header)
{
	return mach_read_from_4(blob_header + BTR_BLOB_HDR_PART_LEN);
}

/*获得存有blob数据的下一页ID*/
static ulint btr_blob_get_next_page_no(byte* blob_header)
{
	return mach_read_from_4(blob_header + BTR_BLOB_HDR_NEXT_PAGE_NO);
}

/*存储big rec的列*/
ulint btr_store_big_rec_extern_fields(dict_index_t* index, rec_t* rec, big_rec_t* big_rec_vec, mtr_t* mtr)
{
	byte*	data;
	ulint	local_len;
	ulint	extern_len;
	ulint	store_len;
	ulint	page_no;
	page_t*	page;
	ulint	space_id;
	page_t*	prev_page;
	page_t*	rec_page;
	ulint	prev_page_no;
	ulint	hint_page_no;
	ulint	i;
	mtr_t	mtr;

	ut_ad(mtr_memo_contains(local_mtr, dict_tree_get_lock(index->tree), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(local_mtr, buf_block_align(data), MTR_MEMO_PAGE_X_FIX));
	ut_a(index->type & DICT_CLUSTERED);

	space_id = buf_frame_get_space_id(rec);

	for(i = 0; i < big_rec_vec->n_fields; i ++){
		data = rec_get_nth_field(rec, big_rec_vec->fields[i].field_no, &local_len);

		ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);
		local_len -= BTR_EXTERN_FIELD_REF_SIZE;

		extern_len = big_rec_vec->fields[i].len;

		ut_a(extern_len > 0);
		prev_page_no = FIL_NULL;

		while(extern_len > 0){
			mtr_start(&mtr);

			/*定位分配页的hint id*/
			if(prev_page_no = FIL_NULL)
				hint_page_no = buf_frame_get_page_no(rec) + 1;
			else
				hint_page_no = prev_page_no + 1;

			/*在btree 分配一个page*/
			page = btr_page_alloc(index->tree, hint_page_no, FSP_NO_DIR, 0, &mtr);
			if(page == NULL){
				mtr_commit(&mtr);
				return DB_OUT_OF_FILE_SPACE;
			}

			page_no = buf_frame_get_page_no(page);
			if(prev_page_no != FIL_NULL){
				prev_page = buf_page_get(space_id, prev_page_no, RW_X_LATCH, &mtr);

				buf_page_dbg_add_level(prev_page, SYNC_EXTERN_STORAGE);
				/*将page no写入到前一页的BTR_BLOB_HDR_NEXT_PAGE_NO中*/
				mlog_write_ulint(prev_page + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO, page_no, MLOG_4BYTES, &mtr);
			}

			/*确定存储的数据长度*/
			if(extern_len > (UNIV_PAGE_SIZE - FIL_PAGE_DATA - BTR_BLOB_HDR_SIZE - FIL_PAGE_DATA_END))
				store_len = UNIV_PAGE_SIZE - FIL_PAGE_DATA - BTR_BLOB_HDR_SIZE - FIL_PAGE_DATA_END;
			else
				store_len = extern_len;
			/*将数据写入到page中*/
			mlog_write_string(page + FIL_PAGE_DATA + BTR_BLOB_HDR_SIZE, big_rec_vec->fields[i].data + big_rec_vec->fields[i].len - extern_len,
				store_len, &mtr);

			/*写入blob列存储在本页的数据长度*/
			mlog_write_ulint(page + FIL_PAGE_DATA + BTR_BLOB_HDR_PART_LEN, store_len, MLOG_4BYTES, &mtr);
			/*因为还不能确定需要更多的页存储本列，所以这里暂时填写FIL_NULL,当下一个页生成时，会更新此值*/
			mlog_write_ulint(page + FIL_PAGE_DATA+ BTR_BLOB_HDR_NEXT_PAGE_NO,FIL_NULL, MLOG_4BYTES, &mtr);

			extern_len -= store_len;
			rec_page = buf_page_get(space_id, buf_frame_get_page_no(data), RW_X_LATCH, &mtr);

			buf_page_dbg_add_level(rec_page, SYNC_NO_ORDER_CHECK);

			mlog_write_ulint(data + local_len + BTR_EXTERN_LEN, 0, MLOG_4BYTES, &mtr);
			mlog_write_ulint(data + local_len + BTR_EXTERN_LEN + 4, big_rec_vec->fields[i].len - extern_len, MLOG_4BYTES, &mtr);
			
			/*写入field起始的页位置信息(space id, page no, offset)到对应rec列上*/
			if(prev_page_no == FIL_NULL){
				mlog_write_ulint(data + local_len
					+ BTR_EXTERN_SPACE_ID, space_id, MLOG_4BYTES, &mtr);

				mlog_write_ulint(data + local_len
					+ BTR_EXTERN_PAGE_NO, page_no, MLOG_4BYTES, &mtr);

				mlog_write_ulint(data + local_len
					+ BTR_EXTERN_OFFSET, FIL_PAGE_DATA, MLOG_4BYTES, &mtr);

				/*设置一个列多个页存储的标识*/
				rec_set_nth_field_extern_bit(rec, big_rec_vec->fields[i].field_no,TRUE, &mtr);
			}

			prev_page_no = page_no;
			mtr_commit(&mtr);
		}
	}

	return DB_SUCCESS;
}

/*将blob占用的page释放*/
void btr_free_externally_stored_field(dict_index_t* index, byte* data, ulint local_len, ibool do_not_free_inherited, mtr_t* local_mtr)
{
	page_t*	page;
	page_t*	rec_page;
	ulint	space_id;
	ulint	page_no;
	ulint	offset;
	ulint	extern_len;
	ulint	next_page_no;
	ulint	part_len;
	mtr_t	mtr;

	ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);
	ut_ad(mtr_memo_contains(local_mtr, dict_tree_get_lock(index->tree), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(local_mtr, buf_block_align(data), MTR_MEMO_PAGE_X_FIX));

	local_len -= BTR_EXTERN_FIELD_REF_SIZE;
	for(;;){
		mtr_start(&mtr);

		/*找到记录所在的页*/
		rec_page = buf_page_get(buf_frame_get_space_id(data), buf_frame_get_page_no(data), RW_X_LATCH, &mtr);
		buf_page_dbg_add_level(rec_page, SYNC_NO_ORDER_CHECK);

		space_id = mach_read_from_4(data + local_len + BTR_EXTERN_SPACE_ID);
		page_no = mach_read_from_4(data + local_len + BTR_EXTERN_PAGE_NO);
		offset = mach_read_from_4(data + local_len + BTR_EXTERN_OFFSET);

		/*获得外部数据长度*/
		extern_len = mach_read_from_4(data + local_len + BTR_EXTERN_LEN + 4);
		if(extern_len == 0){
			mtr_commit(&mtr);
			return ;
		}

		/*记录列没有将数据分到其他页上存储*/
		if(mach_read_from_1(data + local_len + BTR_EXTERN_LEN) & BTR_EXTERN_OWNER_FLAG){
			mtr_commit(&mtr);
			return ;
		}

		/*已经被回滚了，不需要free*/
		if(do_not_free_inherited &&  mach_read_from_1(data + local_len + BTR_EXTERN_LEN) & BTR_EXTERN_INHERITED_FLAG){
			mtr_commit(&mtr);
			return;
		}

		page = buf_page_get(space_id, page_no, RW_X_LATCH, &mtr);
		buf_page_dbg_add_level(page, SYNC_EXTERN_STORAGE);

		/*获得下一个页ID*/
		next_page_no = mach_read_from_4(page + FIL_PAGE_DATA + BTR_BLOB_HDR_NEXT_PAGE_NO);
		part_len = btr_blob_get_part_len(page + FIL_PAGE_DATA);

		ut_a(extern_len >= part_len);

		/*释放blob列占用的页*/
		btr_page_free_low(index->tree, page, 0, &mtr);

		mlog_write_ulint(data + local_len + BTR_EXTERN_PAGE_NO, next_page_no, MLOG_4BYTES, &mtr);
		mlog_write_ulint(data + local_len + BTR_EXTERN_LEN + 4, extern_len - part_len, MLOG_4BYTES, &mtr);

		/*对blob列完整性的判断*/
		if(next_page_no == FIL_NULL)
			ut_a(extern_len - part_len == 0);

		if(extern_len - part_len == 0)
			ut_a(next_page_no == FIL_NULL);

		mtr_commit(&mtr);
	}
}

/*释放rec中所有blob列占用的页空间*/
void btr_rec_free_externally_stored_fields(dict_index_t* index, rec_t* rec, ibool do_not_free_inherited, mtr_t* mtr)
{
	ulint	n_fields;
	byte*	data;
	ulint	len;
	ulint	i;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(rec), MTR_MEMO_PAGE_X_FIX));

	if(rec_get_data_size(rec) <= REC_1BYTE_OFFS_LIMIT)
		return;

	n_fields = rec_get_n_fields(rec);
	for(i = 0; i < n_fields; i ++){
		if (rec_get_nth_field_extern_bit(rec, i)){
			data = rec_get_nth_field(rec, i, &len);
			btr_free_externally_stored_field(index, data, len, do_not_free_inherited, mtr);
		}
	}
}

/*对update中的blob field占用的页进行释放*/
static void btr_rec_free_updated_extern_fields(dict_index_t* index, rec_t* rec, upd_t* update, ibool do_not_free_inherited, mtr_t* mtr)
{
	upd_field_t*	ufield;
	ulint		n_fields;
	byte*		data;
	ulint		len;
	ulint		i;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(rec), MTR_MEMO_PAGE_X_FIX));

	if(rec_get_data_size(rec) < REC_1BYTE_OFFS_LIMIT)
		return ;

	n_fields = upd_get_n_fields(update);

	for (i = 0; i < n_fields; i++) {
		ufield = upd_get_nth_field(update, i);

		if (rec_get_nth_field_extern_bit(rec, ufield->field_no)) {
			data = rec_get_nth_field(rec, ufield->field_no, &len);
			btr_free_externally_stored_field(index, data, len, do_not_free_inherited, mtr);
		}
	}
}

/*对大列进行拷贝*/
byte* btr_copy_externally_stored_field(ulint* len, byte* data, ulint local_len, mem_heap_t* heap)
{
	page_t*	page;
	ulint	space_id;
	ulint	page_no;
	ulint	offset;
	ulint	extern_len;
	byte*	blob_header;
	ulint	part_len;
	byte*	buf;
	ulint	copied_len;
	mtr_t	mtr;

	ut_a(local_len >= BTR_EXTERN_FIELD_REF_SIZE);

	local_len -= BTR_EXTERN_FIELD_REF_SIZE;
	/*获得blob占用页的位置*/
	space_id = mach_read_from_4(data + local_len + BTR_EXTERN_SPACE_ID);
	page_no = mach_read_from_4(data + local_len + BTR_EXTERN_PAGE_NO);
	offset = mach_read_from_4(data + local_len + BTR_EXTERN_OFFSET);

	extern_len = mach_read_from_4(data + local_len + BTR_EXTERN_LEN + 4);
	buf = mem_heap_alloc(heap, local_len + extern_len);
	/*先拷贝field index*/
	ut_memcpy(buf, data, local_len);
	copied_len = local_len;
	if (extern_len == 0){
		*len = copied_len;
		return(buf);
	}

	for(;;){
		mtr_start(&mtr);

		page = buf_page_get(space_id, page_no, RW_X_LATCH, &mtr);
		buf_page_dbg_add_level(page, SYNC_EXTERN_STORAGE);

		blob_header = page + offset;
		part_len = btr_blob_get_part_len(blob_header);
		/*拷贝field data*/
		ut_memcpy(buf + copied_len, blob_header + BTR_BLOB_HDR_SIZE, part_len);
		copied_len += part_len;

		/*获得下一个page的ID*/
		page_no = btr_blob_get_next_page_no(blob_header);

		offset = FIL_PAGE_DATA;

		mtr_commit(&mtr);
		/*已经没有更多存有blob列数据的页了，拷贝完成*/
		if (page_no == FIL_NULL) {
			ut_a(copied_len == local_len + extern_len);
			*len = copied_len;
			return(buf);
		}

		ut_a(copied_len < local_len + extern_len);
	}
}

/*拷贝rec中第no列的数据，这个一定是BLOB列*/
byte* btr_rec_copy_externally_stored_field(rec_t* rec, ulint no, ulint* len, mem_heap_t* heap)
{
	ulint	local_len;
	byte*	data;

	ut_a(rec_get_nth_field_extern_bit(rec, no));

	data = rec_get_nth_field(rec, no, &local_len);
	return btr_copy_externally_stored_field(len, data, local_len, heap);
}



#include "btr0pcur.h"
#include "ut0byte.h"
#include "rem0cmp.h"


btr_pcur_t* btr_pcur_create_for_mysql()
{
	btr_pcur_t*	pcur;

	pcur = mem_alloc(sizeof(btr_pcur_t));

	pcur->btr_cur.index = NULL;
	btr_pcur_init(pcur);

	return pcur;
}

void btr_pcur_free_for_mysql(btr_pcur_t* cursor)
{
	if(cursor->old_rec_buf != NULL){
		mem_free(cursor->old_rec_buf);

		cursor->old_rec = NULL;
		cursor->old_rec_buf = NULL;
	}

	cursor->btr_cur.page_cur.rec = NULL;
	cursor->old_rec = NULL;
	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

	cursor->latch_mode = BTR_NO_LATCHES;
	cursor->pos_state = BTR_PCUR_NOT_POSITIONED;

	mem_free(cursor);
}

/*将pcursor指向的记录和修改时间进行保存到old_stored和old_rec当中*/
void btr_pcur_store_position(btr_pcur_t* cursor, mtr_t* mtr)
{
	page_cur_t*		page_cursor;
	rec_t*			rec;
	dict_tree_t*	tree;
	page_t*			page;

	ut_a(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	tree = btr_cur_get_tree(btr_pcur_get_btr_cur(cursor));
	/*获得page cursor*/
	page_cursor = btr_pcur_get_page_cur(cursor);
	/*获得pcursor指向的记录*/
	rec = page_cur_get_rec(page_cursor);
	/*获得对应的page*/
	page = buf_frame_align(rec);

	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_S_FIX) 
		|| mtr_memo_contains(mtr, buf_block_align(page),MTR_MEMO_PAGE_X_FIX));

	ut_a(cursor->latch_mode != BTR_NO_LATCHES);

	/*没有用户有效记录,为empty tree*/
	if(page_get_n_recs(page) == 0){
		ut_a(btr_page_get_next(page, mtr) == FIL_NULL && btr_page_get_prev(page, mtr) == FIL_NULL);
		
		/*rec记录为supremum*/
		if(rec == page_get_supremum_rec(page)){
			cursor->rel_pos = BTR_PCUR_AFTER_LAST_IN_TREE;
			cursor->old_stored = BTR_PCUR_OLD_STORED;
		}
		else{ /*rec为infimum*/
			cursor->rel_pos = BTR_PCUR_BEFORE_FIRST_IN_TREE;
			cursor->old_stored = BTR_PCUR_OLD_STORED;
		}
		return;
	}

	/*如果还有用户记录*/
	if(rec == page_get_supremum_rec(page)){ 
		/*已经到页末尾，指向最后一条有效的记录*/
		rec = page_rec_get_prev(rec);
		cursor->rel_pos = BTR_PCUR_AFTER;
	}
	else if(rec == page_get_infimum_rec(page)){
		/*已经到页头，指向第一条有效的记录*/
		rec = page_rec_get_next(rec);
		cursor->rel_pos = BTR_PCUR_BEFORE;
	}
	else
		cursor->rel_pos = BTR_PCUR_ON;

	/*将记录和操作时刻记录下来*/
	cursor->old_stored = BTR_PCUR_OLD_STORED;
	cursor->old_rec = dict_tree_copy_rec_order_prefix(tree, rec, &(cursor->old_rec_buf), &(cursor->buf_size));
	cursor->modify_clock = buf_frame_get_modify_clock(page);
}

/*将pcur_donate中的position拷贝到pcur_receive当中*/
void btr_pcur_copy_stored_position(btr_pcur_t* pcur_receive, btr_pcur_t* pcur_donate)
{
	if(pcur_receive->old_rec_buf != NULL)
		mem_free(pcur_receive->old_rec_buf);

	/*拷贝pcursor中的内容*/
	ut_memcpy((byte*)pcur_receive, (byte*)pcur_donate, sizeof(btr_pcur_t));

	/*拷贝old_rec_buf*/
	if(pcur_donate->old_rec_buf != NULL){
		pcur_receive->old_rec_buf = mem_alloc(pcur_donate->buf_size);
		ut_memcpy(pcur_receive->old_rec_buf, pcur_donate->old_rec_buf, pcur_donate->buf_size);
		/*保存最近一条操作的记录内容*/
		pcur_receive->old_rec = pcur_receive->old_rec_buf + (pcur_donate->old_rec - pcur_donate->old_rec_buf);
	}
}

/*重新保存pcursor position*/
ibool btr_pcur_restore_position(ulint latch_mode, btr_pcur_t* cursor, mtr_t* mtr)
{
	dict_tree_t*	tree;
	page_t*		page;
	dtuple_t*	tuple;
	ulint		mode;
	ulint		old_mode;
	ibool		from_left;
	mem_heap_t*	heap;

	ut_a(cursor->pos_state == BTR_PCUR_WAS_POSITIONED || cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_a(cursor->old_stored == BTR_PCUR_OLD_STORED);

	/*已经到了btree树的最左边或者最右边*/
	if(cursor->rel_pos == BTR_PCUR_AFTER_LAST_IN_TREE || cursor->rel_pos == BTR_PCUR_BEFORE_FIRST_IN_TREE){
		if(cursor->rel_pos == BTR_PCUR_BEFORE_FIRST_IN_TREE) /*cursor指向最左边位置只能从最左边向右*/
			from_left = TRUE;
		else /*cursor最右边，只能从最右边向左边*/
			from_left = FALSE;

		/*定位到对应的位置*/
		btr_cur_open_at_index_side(from_left, btr_pcur_get_btr_cur(cursor)->index, latch_mode, btr_pcur_get_btr_cur(cursor), mtr);

		return FALSE;
	}

	ut_a(cursor->old_rec);

	page = btr_cur_get_page(btr_pcur_get_btr_cur(cursor));
	/*只是搜索或者修改btree的叶子节点*/
	if(latch_mode == BTR_SEARCH_LEAF || latch_mode == BTR_MODIFY_LEAF){
		/*尝试乐观方式进行*/
		if(buf_page_optimistic_get(latch_mode, page, cursor->modify_clock, mtr)){
			cursor->pos_state = BTR_PCUR_IS_POSITIONED;
			buf_page_dbg_add_level(page, SYNC_TREE_NODE);

			if(cursor->rel_pos == BTR_PCUR_ON){
				cursor->latch_mode = latch_mode;

				/*上次操作的记录就是cursor指向的记录*/
				ut_ad(cmp_rec_rec(cursor->old_rec, btr_pcur_get_rec(cursor),
					dict_tree_find_index(btr_cur_get_tree(btr_pcur_get_btr_cur(cursor)), btr_pcur_get_rec(cursor))) == 0); 

				return TRUE;
			}

			return FALSE;
		}
	}

	heap = mem_heap_create(256);

	tree = btr_cur_get_tree(btr_pcur_get_btr_cur(cursor));
	/*通过old_rec构建一个tuple记录*/
	tuple = dict_tree_build_data_tuple(tree, cursor->old_rec, heap);

	old_mode = cursor->search_mode;
	/*确定记录匹配的方式*/
	if(cursor->rel_pos == BTR_PCUR_ON)
		mode = PAGE_CUR_LE; /*小于等于*/
	else if(cursor->rel_pos == BTR_PCUR_AFTER)
		mode = PAGE_CUR_G;  /*大于*/
	else 
		mode = PAGE_CUR_L; /*小于*/

	/*通过tuple定位到btree对应的位置*/
	btr_pcur_open_with_no_init(btr_pcur_get_btr_cur(cursor)->index, tuple, mode, latch_mode, cursor, 0, mtr);

	cursor->old_stored = BTR_PCUR_OLD_STORED;

	cursor->search_mode = old_mode;
	/*tuple与cursor执行的记录是一致的内容,只需要跟新下modify clock*/
	if (cursor->rel_pos == BTR_PCUR_ON && btr_pcur_is_on_user_rec(cursor, mtr)
		&& 0 == cmp_dtuple_rec(tuple, btr_pcur_get_rec(cursor))){
			cursor->modify_clock = buf_frame_get_modify_clock(buf_frame_buf(btr_pcur_get_rec(cursor)));
			mem_heap_free(heap);

			return TRUE;
	}

	mem_heap_free(heap);
	/*从新更新position*/
	btr_pcur_store_position(cursor, mtr);

	return FALSE;
}

/*释放cursor指向page的latch(BTR_LEAF_SEARCH、BTR_LEAF_MODIFY)*/
void btr_pcur_release_leaf(btr_pcur_t* cursor, ulint latch_mode, mtr_t* mtr)
{
	page_t* page;
	
	ut_a(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	/*获得cursor对应page*/
	page = btr_cur_get_page(btr_pcur_get_btr_cur(cursor));
	/*释放持有的latch*/
	btr_leaf_page_release(page, cursor->latch_mode, mtr);

	cursor->latch_mode = BTR_NO_LATCHES;

	cursor->pos_state = BTR_PCUR_WAS_POSITIONED;
}

/*cursor移向下一个page*/
void btr_pcur_move_to_next_page(btr_pcur_t* cursor, mtr_t* mtr)
{
	ulint	next_page_no;
	ulint	space;
	page_t*	page;
	page_t*	next_page;

	ut_a(cursor->pos_state == BTR_PCUR_IS_POSITIONED);	
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);
	ut_ad(btr_pcur_is_after_last_on_page(cursor, mtr));	

	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
	page = btr_pcur_get_page(cursor);

	/*获得同一层上下个page no*/
	next_page_no = btr_page_get_next(page, mtr);
	space = buf_frame_get_space_id(page);

	ut_ad(next_page_no != FIL_NULL);

	/*获得下一个page的指针*/
	next_page = btr_page_get(space, next_page_no, cursor->latch_mode, mtr);
	/*释放本page持有的latch*/
	btr_leaf_page_release(page, cursor->latch_mode, mtr);

	/*定位到infimum上，因为是向后移动，所以cursor会定位在后一页的infimum上*/
	page_cur_set_before_first(next_page, btr_pcur_get_page_cur(cursor));
}

void btr_pcur_move_backward_from_page(btr_pcur_t* cursor, mtr_t* mtr)
{
	ulint	prev_page_no;
	ulint	space;
	page_t*	page;
	page_t*	prev_page;
	ulint	latch_mode;
	ulint	latch_mode2;

	ut_a(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	ut_ad(btr_pcur_is_before_first_on_page(cursor, mtr));	
	ut_ad(!btr_pcur_is_before_first_in_tree(cursor, mtr));	

	latch_mode = cursor->latch_mode;
	/*确定mode2*/
	if(latch_mode == BTR_SEARCH_LEAF)
		latch_mode2 = BTR_SEARCH_PREV;
	else if(latch_mode == BTR_MODIFY_LEAF)
		latch_mode2 = BTR_MODIFY_PREV;
	else{ /*这个条件完全是为了排除编译器警告*/
		latch_mode2 = 0;
		ut_error;
	}

	btr_pcur_store_position(cursor, mtr);

	mtr_commit(mtr);

	mtr_start(mtr);

	btr_pcur_restore_position(latch_mode2, cursor, mtr);
	page = btr_pcur_get_page(cursor);

	prev_page_no = btr_page_get_prev(page, mtr);
	space = buf_frame_get_space_id(page);

	/*已经到页的头一条记录了，再移动就指向上一页的最后一条记录*/
	if(btr_pcur_is_before_first_on_page(cursor, mtr) && prev_page_no != FIL_NULL){
		prev_page = btr_pcur_get_btr_cur(cursor)->left_page;
		btr_leaf_page_release(page, latch_mode, mtr);
		page_cur_set_after_last(prev_page, btr_pcur_get_page_cur(cursor));
	}
	else if(prev_page_no != FIL_NULL){
		prev_page = btr_pcur_get_btr_cur(cursor)->left_page;
		btr_leaf_page_release(prev_page, latch_mode, mtr);
	}

	cursor->latch_mode = latch_mode;
	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/*移向pcursor指向的前一条记录*/
ibool btr_pcur_move_to_prev(btr_pcur_t*	cursor,mtr_t* mtr)	
{
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);
	
	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

	/*cursor已经处于页的最前面*/
	if (btr_pcur_is_before_first_on_page(cursor, mtr)){
		if (btr_pcur_is_before_first_in_tree(cursor, mtr)) /*已经到树当前层的最前面，无法再向前移动*/
			return FALSE;

		btr_pcur_move_backward_from_page(cursor, mtr);
		return TRUE;
	}

	btr_pcur_move_to_prev_on_page(cursor, mtr);

	return(TRUE);
}

/*根据tuple打开并定位一个pcursor,如果mode =PAGE_CUR_GE或PAGE_CUR_G，需要检查是否跨页*/
void btr_pcur_open_on_user_rec(
	dict_index_t*	index,	/* in: index */
	dtuple_t*	tuple,		/* in: tuple on which search done */
	ulint		mode,		/* in: PAGE_CUR_L, ... */
	ulint		latch_mode,	/* in: BTR_SEARCH_LEAF or BTR_MODIFY_LEAF */
	btr_pcur_t*	cursor, 	/* in: memory buffer for persistent cursor */
	mtr_t*		mtr)		/* in: mtr */
{
	btr_pcur_open(index, tuple, mode, latch_mode, cursor, mtr);

	if ((mode == PAGE_CUR_GE) || (mode == PAGE_CUR_G)){
		if (btr_pcur_is_after_last_on_page(cursor, mtr)) 
			btr_pcur_move_to_next_user_rec(cursor, mtr);/*pcursor指向page的supremum上，则移动到下一条记录上*/
	} 
	else{
		ut_ad((mode == PAGE_CUR_LE) || (mode == PAGE_CUR_L));
		ut_error;
	}
}


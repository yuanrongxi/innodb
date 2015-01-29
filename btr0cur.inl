#include "btr0btr.h"

/*通过btr cur获得page cursor*/
UNIV_INLINE page_cur_t* btr_cur_get_page_cur(btr_cur_t* cursor)
{
	return &(cursor->page_cur);
}

/*通过btree cursor获得对应的记录*/
UNIV_INLINE rec_t* btr_cur_get_rec(btr_cur_t* cursor)
{
	return page_cur_get_rec(&(cursor->page_cur));
}

UNIV_INLINE void  btr_cur_invalidate(btr_cur_t* cursor)
{
	page_cur_invalidate(&(cursor->page_cur));
}

/*通过btree cursor获得对应的page页*/
UNIV_INLINE page_t* btr_cur_get_page(btr_cur_t* cursor)
{
	return buf_frame_align(page_cur_get_rec(&(cursor->page_cur)));
}

/*通过btree cursor获得对应的索引对象*/
UNIV_INLINE dict_tree_t* btr_cur_get_tree(btr_cur_t* cursor)
{
	return cursor->index->tree;
}

/*设置btree cursor对应的page记录记录*/
UNIV_INLINE void btr_cur_position(dict_index_t* index, rec_t* rec, btr_cur_t* cursor)
{
	page_cur_position(rec, btr_cur_get_page_cur(cursor));
}

/*对btree cursor对应的page判断是否可以合并*/
UNIV_INLINE ibool btr_cur_compress_recommendation(btr_cur_t* cursor, mtr_t* mtr)
{
	page_t* page;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(btr_cur_get_page(cursor)), MTR_MEMO_PAGE_X_FIX));

	page = btr_cur_get_page(cursor);

	/*page的填充数据低于填充因子或者没有兄弟页*/
	if((page_get_data_size(page) < BTR_CUR_PAGE_COMPRESS_LIMIT) || 
		(btr_page_get_next(page, mtr) == FIL_NULL && btr_page_get_prev(page, mtr) == FIL_NULL)){
			/*已经是root page了，不能进行合并*/
			if(dict_tree_get_page(cursor->index->tree) == buf_frame_get_page_no(page))
				return FALSE;

			return TRUE;
	}

	return FALSE;
}

/*检查删除cursor对应的记录是否会触发cursor对应页的合并，如果不会返回TRUE*/
UNIV_INLINE ibool btr_cur_can_delete_without_compress(btr_cur_t* cursor, mtr_t* mtr)
{
	ulint rec_size;
	page_t* page;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(btr_cur_get_page(cursor)), MTR_MEMO_PAGE_X_FIX));

	rec_size = rec_get_size(btr_cur_get_rec(cursor));
	page = btr_cur_get_page(cursor);

	/*如果删除当前cursor指向的记录后可以触发合并，（数据占用量小于填充因子 或者 当前page无兄弟 或者 page只剩下1条记录）*/
	if((page_get_data_size(page) - rec_size < BTR_CUR_PAGE_COMPRESS_LIMIT)
		|| ((btr_page_get_next(page, mtr) == FIL_NULL) && (btr_page_get_prev(page, mtr) == FIL_NULL))
		|| page_get_n_rec(page) < 2){
			if(dict_tree_get_page(cursor->index->tree) == buf_frame_get_page_no(page))
				return TRUE;

			return FALSE;
	}

	return TRUE;
}


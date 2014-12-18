#include "page0page.h"

/*通过游标找到page的指针*/
UNIV_INLINE page_t* page_cur_get_page(page_cur_t* cur)
{
	ut_ad(cur);

	return buf_frame_align(cur->rec);
}

/*获取游标指向的记录*/
UNIV_INLINE rec_t* page_cur_get_rec(page_cur_t* cur)
{
	ut_ad(cur);

	return cur->rec;
}

/*将page的起始记录作为游标指向的记录*/
UNIV_INLINE void page_cur_set_before_first(page_t* page, page_cur_t* cur)
{
	cur->rec = page_get_infimum_rec(page);
}

/*将page的结束记录作为游标指向的记录*/
UNIV_INLINE void page_cur_set_after_last(page_t* page, page_cur_t* cur)
{
	cur->rec = page_get_supremum_rec(page);
}

UNIV_INLINE ibool page_cur_is_before_first(page_cur_t* cur)
{
	if(page_get_infimum_rec(page_cur_get_page(cur)) == cur->rec)
		return TRUE;

	return FALSE;
}

/*判断游标是否到了页的最后*/
UNIV_INLINE ibool page_cur_is_after_last(page_cur_t* cur)
{
	if(page_get_supremum_rec(page_cur_get_page(cur)) == cur->rec)
		return TRUE;

	return FALSE;
}

UNIV_INLINE void page_cur_position(rec_t* rec, page_cur_t* cur)
{
	ut_ad(rec && cur);

	cur->rec = rec;
}

UNIV_INLINE void page_cur_invalidate(page_cur_t* cur)
{
	ut_ad(cur);

	cur->rec = NULL;
}

/*游标移向下一条记录*/
UNIV_INLINE void page_cur_move_to_next(page_cur_t* cur)
{
	ut_ad(!page_cur_is_after_last(cur));
	cur->rec = page_rec_get_next(cur->rec);
}
/*游标移向上一条记录*/
UNIV_INLINE void page_cur_move_to_prev(page_cur_t* cur)
{
	ut_ad(!page_cur_is_before_first(cur));
	cur->rec = page_rec_get_prev(cur->rec);
}

UNIV_INLINE ulint page_cur_search(page_t* page, dtuple_t* tuple, ulint mode, page_cur_t* cursor)
{
	ulint		low_matched_fields = 0;
	ulint		low_matched_bytes = 0;
	ulint		up_matched_fields = 0;
	ulint		up_matched_bytes = 0;

	ut_ad(dtuple_check_typed(tuple));

	page_cur_search_with_match(page, tuple, mode, &up_matched_fields, &up_matched_bytes,
		&low_matched_fields, &low_matched_bytes);

	return low_matched_fields;
}

UNIV_INLINE rec_t* page_cur_tuple_insert(page_cur_t* cursor, dtuple_t* tuple, mtr_t* mtr)
{
	ulint data_size;
	ut_ad(dtuple_check_typed(tuple));

	data_size = dtuple_get_data_size(tuple);

	return page_cur_insert_rec_low(cursor, tuple, data_size, NULL, mtr);
}

UNIV_INLINE rec_t* page_cur_rec_insert(page_cur_t* cursor, rec_t* rec, mtr_t* mtr)
{
	return page_cur_insert_rec_low(cursor, NULL, 0, rec, mtr);
}



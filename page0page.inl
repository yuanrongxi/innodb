#include "mach0data.h"
#include "rem0cmp.h"
#include "mtr0log.h"

#ifdef UNIV_MATERIALIZE
#undef UNIV_INLINE
#define UNIV_INLINE
#endif

UNIV_INLINE dulint page_get_max_trx_id(page_t* page)
{
	ut_ad(page);

	return mach_read_from_8(page + PAGE_HEADER + PAGE_MAX_TRX_ID);
}

UNIV_INLINE void page_update_max_trx_id(page_t* page, dulint trx_id)
{
	ut_ad(page);
	if(ut_dulint_cmp(trx_id, page_get_max_trx_id(page)) >= 0)
		page_set_max_trx_id(page, trx_id);

}

UNIV_INLINE ulint page_header_get_field(page_t* page, ulint field)
{
	ut_ad(page);
	ut_ad(field <= PAGE_INDEX_ID);
	
	return mach_read_from_2(page + PAGE_HEADER + field);
}

UNIV_INLINE void page_header_set_field(page_t* page, ulint field, ulint val)
{
	ut_ad(page);
	ut_ad(field <= PAGE_N_RECS);
	ut_ad(val < UNIV_PAGE_SIZE);

	mach_write_to_2(page + PAGE_HEADER + field, val);
}

UNIV_INLINE byte* page_header_get_ptr(page_t* page, ulint field)
{
	ulint offs;

	ut_ad(page);
	ut_ad(field == PAGE_FREE || field == PAGE_LAST_INSERT || field == PAGE_HEAP_TOP);

	offs = page_header_get_field(page, field);
	ut_ad(field != PAGE_HEAP_TOP || offs != 0);

	if(offs == 0)
		return NULL;

	return page + offs;
}

UNIV_INLINE byte* page_header_set_ptr(page_t* page,ulint field, byte* ptr)
{
	ulint offs;

	ut_ad(page);
	ut_ad((field == PAGE_FREE)
		|| (field == PAGE_LAST_INSERT)
		|| (field == PAGE_HEAP_TOP));

	if(ptr == NULL)
		offs = 0;
	else
		offs = ptr - page;

	ut_ad(field != PAGE_HEAP_TOP || offs != 0);

	page_header_set_field(page, field, offs);
}

UNIV_INLINE void page_header_reset_last_insert(page_t*, page, mtr_t* mtr)
{
	ut_ad(page != NULL && mtr != NULL);

	mlog_write_ulint(page + PAGE_HEADER + PAGE_LAST_INSERT, 0, MLOG_2BYTES, mtr);
}

UNIV_INLINE rec_t* page_get_infimum_rec(page_t* page)
{
	ut_ad(page);

	return page + PAGE_INFIMUM;
}

UNIV_INLINE rec_t* page_get_supremum_rec(page_t* page)
{
	ut_ad(page);

	return page + PAGE_SUPREMUM;
}

/*判断一条记录是否是用户的记录*/
UNIV_INLINE ibool page_rec_is_user_rec(rec_t* rec)
{
	ut_ad(rec);

	if(rec == page_get_suremum_rec(buf_frame_align(rec)))
		return FALSE;

	if(rec == page_get_infimum_rec(buf_frame_align(rec)))
		return FALSE;

	return TRUE;
}

UNIV_INLINE ibool page_rec_is_supremum(rec_t* rec)
{
	if(rec == page_get_suremum_rec(buf_frame_align(rec)))
		return TRUE;

	return FALSE;
}

UNIV_INLINE ibool page_rec_is_infimum(rec_t* rec)
{
	if(rec == page_get_infimum_rec(buf_frame_align(rec)))
		return TRUE;

	return FALSE;
}

UNIV_INLINE ibool page_rec_is_first_user_rec(rec_t* rec)
{
	if(rec == page_get_suremum_rec(buf_frame_align(rec)))
		return FALSE;

	if(rec == page_rec_get_next(page_get_infimum_rec(buf_frame_align(rec))))
		return TRUE;

	return FALSE;
}

UNIV_INLINE ibool page_rec_is_last_user_rec(rec_t* rec)
{
	ut_ad(rec);

	if(rec == page_get_suremum_rec(buf_frame_align(rec)))
		return FALSE;

	if(page_rec_get_next(rec) == page_get_suremum_rec(buf_frame_align(rec)))
		return TRUE;

	return FALSE;
}

UNIV_INLINE int page_cmp_dtuple_rec_with_match(dtuple_t* dtuple, rec_t* rec, ulint* matched_fields, ulint* matched_bytes)
{
	page_t* page;

	ut_ad(dtuple_check_typed(dtuple));

	page = buf_frame_align(rec);
	if(rec == page_get_infimum_rec(page))
		return 1;
	else if(rec == page_get_supremum_rec(page))
		return -1;
	else
		return cmp_dtuple_rec_with_match(dtuple, rec, matched_fields, matched_bytes);

}

UNIV_INLINE ulint page_get_n_recs(page_t* page)
{
	return page_header_get_field(page, PAGE_N_RECS);
}

UNIV_INLINE ulint page_dir_get_n_slots(page_t* page)
{
	ut_ad(page);

	return page_header_get_field(page, PAGE_N_DIR_SLOTS);
}

/*page directory是从后向前排列？*/
UNIV_INLINE page_dir_slot_t* page_dir_get_nth_slot(page_t* page, ulint n)
{
	ut_ad(page_header_get_field(page, PAGE_N_DIR_SLOTS) > n);

	return page + UNIV_PAGE_SIZE - PAGE_DIR - ((n + 1) * PAGE_DIR_SLOT_SIZE);
}

/*检查记录是否是在page*/
UNIV_INLINE ibool page_rec_check(rec_t* rec)
{
	page_t* page;
	ut_a(rec);

	page = buf_frame_align(rec);
	ut_a(rec <= page_header_get_ptr(page, PAGE_HEAP_TOP));
	ut_a(rec >= page + PAGE_DATA);

	return TRUE;
}

/*获得slot对应的记录指针*/
UNIV_INLINE rec_t* page_dir_slot_get_rec(page_dir_slot_t* slot)
{
	return buf_frame_align(slot) + mach_read_from_2(slot);
}

UNIV_INLINE void page_dir_slot_set_rec(page_dir_slot_t* slot, rec_t* rec)
{
	ut_ad(page_rec_check(rec));

	mach_write_to_2(slot, rec - buf_frame_align(rec));
}

UNIV_INLINE ulint page_dir_slot_get_n_owned(page_dir_slot_t* slot)
{
	return rec_get_n_owned(page_dir_slot_get_rec(slot));
}

UNIV_INLINE void page_dir_slot_set_n_owned(page_dir_slot_t* slot, ulint n)
{
	rec_set_n_owned(page_dir_slot_get_rec(slot), n);
}

UNIV_INLINE ulint page_dir_calc_reserved_space(ulint n_recs)
{
	return (PAGE_DIR_SLOT_SIZE * n_recs + PAGE_DIR_SLOT_MIN_N_OWNED - 1) / PAGE_DIR_SLOT_MIN_N_OWNED;
}

UNIV_INLINE rec_t* page_rec_get_next(rec_t* rec)
{
	ulint offs;
	page_t* page;

	ut_ad(page_rec_check(rec));
	
	page = buf_frame_align(rec);

	/*获得下一条记录的偏移量*/
	offs = rec_get_next_offs(rec);
	ut_a(offs < UNIV_PAGE_SIZE);

	if(offs == 0)
		return NULL;

	return page + offs;
}

UNIV_INLINE void page_rec_set_next(rec_t* rec, rec_t* next)
{
	page_t* page;
	
	ut_ad(page_rec_check(rec));
	ut_a(next == NULL || buf_frame_align(rec) == buf_frame_align(next));

	page = buf_frame_align(rec);
	ut_ad(rec != page_get_supremum_rec(page));
	ut_ad(next != page_get_infimum_rec(page));

	if(next == NULL)
		rec_set_next_offs(rec, 0);
	else
		rec_set_next_offs(rec, (ulint)(next - page));
}

UNIV_INLINE rec_t* page_rec_get_prev(rec_t* rec)
{
	page_dir_slot_t*	slot;
	ulint			slot_no;
	rec_t*			rec2;
	rec_t*			prev_rec = NULL;
	page_t*			page;

	ut_ad(page_rec_check(rec));
	page = buf_frame_align(rec);

	ut_ad(rec != page_get_infimum_rec(page));
	
	slot_no = page_dir_find_owner_slot(rec);
	ut_a(slot_no != 0);

	slot = page_dir_get_nth_slot(page, slot_no - 1);
	/*从slot的起始记录开始找，直到找到rec为止,备注：1个slot当中可能存在多个记录*/
	rec2 = page_dir_slot_get_rec(slot);
	while(rec != rec2){
		prev_rec = rec2;
		rec2 = page_dir_slot_get_rec(rec2);
	}

	ut_a(prev_rec);

	return prev_rec;
}

UNIV_INLINE rec_t* page_rec_find_owner_rec(rec_t* rec)
{
	ut_ad(page_rec_ckeck(rec));

	while(rec_get_n_owned(rec) == 0)
		rec = page_rec_get_next(rec);

	return rec;
}

UNIV_INLINE ulint page_get_data_size(page_t* page)
{
	ulint ret;

	ret = (ulint)(page_header_get_field(page, PAGE_HEAP_TOP) - PAGE_SUPREMUM_END - page_header_get_field(page, PAGE_GARBAGE));
	ut_ad(ret < UNIV_PAGE_SIZE);
	
	return ret;
}

UNIV_INLINE ulint page_get_free_space_of_empty()
{
	return (ulint)(UNIV_PAGE_SIZE - PAGE_SUPREMUM_END - PAGE_DIR - 2 * PAGE_DIR_SLOT_SIZE);
}

UNIV_INLINE ulint page_get_max_insert_size(page_t* page, ulint n_recs)
{
	ulint	occupied;
	ulint	free_space;

	occupied = page_header_get_field(page, PAGE_HEAP_TOP) - PAGE_SUPREMUM_END + \
		page_dir_calc_reserved_space(n_recs + (page_header_get_field(page, PAGE_N_HEAP) - 2));

	free_space = page_get_free_space_of_empty();
	if(occupied > free_space)
		return 0;

	return free_space - occupied;
}

UNIV_INLINE ulint page_get_max_insert_size_after_reorganize(page_t* page, ulint n_recs)
{
	ulint	occupied;
	ulint	free_space;

	occupied = page_get_data_size(page)
		+ page_dir_calc_reserved_space(n_recs + page_get_n_recs(page));

	free_space = page_get_free_space_of_empty();

	if (occupied > free_space)
		return(0);


	return free_space - occupied;
}

UNIV_INLINE void page_mem_free(page_t* page, rec_t* rec)
{
	rec_t*	free;
	ulint	garbage;

	free = page_header_get_ptr(page, PAGE_FREE);

	page_rec_set_next(rec, free);
	page_header_set_ptr(page, PAGE_FREE, rec);

	garbage = page_header_get_ptr(page, PAGE_GARBAGE);
	page_header_set_field(page, PAGE_GARBAGE, garbage + rec_get_size(rec));
}

#ifdef UNIV_MATERIALIZE
#undef UNIV_INLINE
#define UNIV_INLINE	UNIV_INLINE_ORIGINAL
#endif






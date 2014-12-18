#ifndef __PAGE0CUR_H_
#define __PAGE0CUR_H_

#include "page0cur.h"

#include "rem0cmp.h"
#include "mtr0log.h"
#include "log0recv.h"

ulint page_cur_short_succ = 0;
ulint page_rnd_ = 976722341;

#ifdef PAGE_CUR_ADAPT

UNIV_INLINE ibool page_cur_try_search_shortcut(page_t* page, dtuple_t* tuple, 
	ulint* iup_matched_fields, ulint* iup_matched_bytes, ulint* ilow_matched_fields, ulint* ilow_matched_bytes, page_cur_t* cursor)
{
	int	cmp;
	rec_t*	rec;
	rec_t*	next_rec;
	ulint	low_match;
	ulint	low_bytes;
	ulint	up_match;
	ulint	up_bytes;

	ut_ad(dtuple_check_typed(tuple));

	rec = page_header_get_ptr(page, PAGE_LAST_INSERT);
	ut_ad(rec);
	ut_ad(page_rec_is_user_rec(rec));

	ut_pair_min(&low_match, &low_bytes, *ilow_matched_fields, *ilow_matched_bytes,*iup_matched_fields, *iup_matched_bytes);

	up_match = low_match;
	up_bytes = low_bytes;

	cmp = page_cmp_dtuple_rec_with_match(tuple, rec, &low_match, &low_bytes);
	if(cmp == -1) /*rec是supremum*/
		return FALSE;

	next_rec = page_rec_get_next(rec);
	cmp = page_cmp_dtuple_rec_with_match(tuple, next_rec, &up_match, &up_bytes);
	if(cmp != -1)
		return FALSE;

	cursor->rec = rec;
	if(next_rec != page_get_supremum_rec(page)){
		*iup_matched_fields = up_match;
		*iup_matched_bytes = up_bytes;
	}

	*ilow_matched_fields = low_match;
	*ilow_matched_bytes = low_bytes;

	return TRUE;
}

void page_cur_search_with_match(page_t* page, dtuple_t* tuple, ulint mode, 
	ulint* iup_matched_fields, ulint* iup_matched_bytes, 
	ulint* ilow_matched_fields, ulint* ilow_matched_bytes, page_cur_t* cursor)
{
	ulint	up;	
	ulint	low;	
	ulint	mid;
	page_dir_slot_t* slot;
	rec_t*	up_rec;
	rec_t*	low_rec;
	rec_t*	mid_rec;
	ulint	up_matched_fields;
	ulint	up_matched_bytes;
	ulint	low_matched_fields;
	ulint	low_matched_bytes;
	ulint	cur_matched_fields;
	ulint	cur_matched_bytes;
	int	cmp;

	ut_ad(page && tuple && iup_matched_fields && iup_matched_bytes && ilow_matched_fields && ilow_matched_bytes && cursor);

	ut_ad(dtuple_validate(tuple));
	ut_ad(dtuple_check_typed(tuple));

	ut_ad((mode == PAGE_CUR_L) || (mode == PAGE_CUR_LE)
		|| (mode == PAGE_CUR_G) || (mode == PAGE_CUR_GE)
		|| (mode == PAGE_CUR_DBG));

#ifdef PAGE_CUR_ADAPT
	if ((page_header_get_field(page, PAGE_LEVEL) == 0)
		&& (mode == PAGE_CUR_LE)
		&& (page_header_get_field(page, PAGE_N_DIRECTION) > 3)
		&& (page_header_get_ptr(page, PAGE_LAST_INSERT))
		&& (page_header_get_field(page, PAGE_DIRECTION) == PAGE_RIGHT)) {

			if (page_cur_try_search_shortcut(page, tuple,
				iup_matched_fields,
				iup_matched_bytes,
				ilow_matched_fields,
				ilow_matched_bytes,
				cursor)) {
					return;
			}
	}

	if (mode == PAGE_CUR_DBG)
		mode = PAGE_CUR_LE;
#endif	

	up_matched_fields  = *iup_matched_fields;
	up_matched_bytes   = *iup_matched_bytes;
	low_matched_fields = *ilow_matched_fields;
	low_matched_bytes  = *ilow_matched_bytes;

	low = 0;
	up = page_dir_get_n_slots(page) - 1;

	/*进行二分查找*/
	while(up - low > 1){
		mid = (low + up) / 2;
		slot = page_dir_get_nth_slot(page, mid);
		mid_rec = page_dir_slot_get_rec(slot);

		ut_pair_min(&cur_matched_fields, &cur_matched_bytes,
			low_matched_fields, low_matched_bytes,
			up_matched_fields, up_matched_bytes);

		if(cmp == -1){
			low = mid;
			low_matched_fields = cur_matched_fields;
			low_matched_bytes = cur_matched_bytes;
		}
		else if(cmp == 1){
			up = mid;
			up_matched_fields = cur_matched_fields;
			up_matched_bytes = cur_matched_bytes; 
		}
		else if((mode == PAGE_CUR_G) || (mode == PAGE_CUR_LE)){
			low = mid;
			low_matched_fields = cur_matched_fields;
			low_matched_bytes = cur_matched_bytes;
		}
		else{
			up = mid;
			up_matched_fields = cur_matched_fields;
			up_matched_bytes = cur_matched_bytes;
		}
	}

	slot = page_dir_get_nth_slot(page, low);
	low_rec = page_dir_slot_get_rec(slot);

	slot = page_dir_get_nth_slot(page, up);
	up_rec = page_dir_slot_get_rec(slot);

	while(page_rec_get_next(low_rec) != up_rec){
		mid_rec = page_rec_get_next(low_rec);

		ut_pair_min(&cur_matched_fields, &cur_matched_bytes,
			low_matched_fields, low_matched_bytes,
			up_matched_fields, up_matched_bytes);

		cmp = cmp_dtuple_rec_with_match(tuple, mid_rec,
			&cur_matched_fields,
			&cur_matched_bytes);

		if (cmp == 1) {
			low_rec = mid_rec;
			low_matched_fields = cur_matched_fields;
			low_matched_bytes = cur_matched_bytes;

		} else if (cmp == -1) {
			up_rec = mid_rec;
			up_matched_fields = cur_matched_fields;
			up_matched_bytes = cur_matched_bytes; 

		} else if ((mode == PAGE_CUR_G) || (mode == PAGE_CUR_LE)) {
			low_rec = mid_rec;
			low_matched_fields = cur_matched_fields;
			low_matched_bytes = cur_matched_bytes;
		} else {
			up_rec = mid_rec;
			up_matched_fields = cur_matched_fields;
			up_matched_bytes = cur_matched_bytes;
		}
	}

	if(mode <= PAGE_CUR_GE)
		cursor->rec = up_rec;
	else
		cursor->rec = low_rec;

	*iup_matched_fields  = up_matched_fields;
	*iup_matched_bytes   = up_matched_bytes;
	*ilow_matched_fields = low_matched_fields;
	*ilow_matched_bytes  = low_matched_bytes;
}

/*随机取一条记录作为游标的指向*/
void page_cur_open_on_rnd_user_rec(page_t* page, page_cur_t* cursor)
{
	ulint rnd;
	rec_t* rec;

	if(page_get_n_recs(page) == 0){
		page_cur_position(page_get_infimum_rec(page), cursor);
		return;
	}

	pag_rnd += 87584577;
	rnd = page_rnd % page_get_n_recs(page);
	rec = page_get_infimum_rec(page);
	rec = page_rec_get_next(rec);
	while(rnd > 0){
		rec = page_rec_get_next(rec);
		rnd --;
	}

	page_cur_position(rec, cursor);
}

#endif





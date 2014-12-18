#ifndef __PAGE0CUR_H_
#define __PAGE0CUR_H_

#include "univ.h"
#include "page0types.h"
#include "page0page.h"
#include "data0data.h"
#include "mtr0mtr.h"

#define PAGE_CUR_ADAPT

/*page 游标的检索模式*/
#define	PAGE_CUR_G		1
#define	PAGE_CUR_GE		2
#define	PAGE_CUR_L		3
#define	PAGE_CUR_LE		4
#define	PAGE_CUR_DBG	5

extern ulint page_cur_short_succ;


UNIV_INLINE page_t* page_cur_get_page(page_cur_t* cur);

UNIV_INLINE rec_t*	page_cur_get_rec(page_cur_t* cur);

UNIV_INLINE void	page_cur_set_before_first(page_t* page, page_cur_t* cur);

UNIV_INLINE ibool	page_cur_is_before_first(page_cur_t* cur);

UNIV_INLINE void	page_cur_position(rec_t* rec, page_cur_t* cur);

UNIV_INLINE void	page_cur_invalidate(page_cur_t* cur);

UNIV_INLINE void	page_cur_move_to_next(page_cur_t* cur);

UNIV_INLINE void	page_cur_move_to_prev(page_cur_t* cur);

UNIV_INLINE rec_t*	page_cur_tuple_insert(page_cur_t* cursor, dtuple_t* tuple, mtr_t* mtr);

UNIV_INLINE rec_t*	page_cur_rec_insert(page_cur_t* cursor, rec_t* rec, mtr_t* mtr);

rec_t*				page_cur_insert_rec_low(page_cur_t* cursor, dtuple_t* tuple, ulint data_size, rec_t* rec, mtr_t* mtr);

void				page_copy_rec_list_end_to_created_page(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr);

void				page_cur_delete_rec(page_cur_t* cursor, mtr_t* mtr);

UNIV_INLINE	ulint	page_cur_search(page_t* page, dtuple_t* tuple, ulint mode, page_cur_t* cursor);

void				page_cur_search_with_match(page_t* page, dtuple_t* tuple, ulint mode, 
								ulint* iup_matched_fields, ulint* iup_matched_bytes, 
								ulint* ilow_matched_fields, ulint* ilow_matched_bytes,
								page_cur_t*	cursor);

void				page_cur_open_on_rnd_user_rec(page_t* page, page_cur_t* cursor);

byte*				page_cur_parse_insert_rec(ibool is_short, byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

byte*				page_parse_copy_rec_list_to_created_page(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

byte*				page_cur_parse_delete_rec(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

struct page_cur_struct
{
	byte*	rec;	/*记录的指针*/
};

#include "page0cur.inl"

#endif






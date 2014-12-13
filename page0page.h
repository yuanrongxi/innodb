#ifndef __PAGE0PAGE_H_
#define __PAGE0PAGE_H_

#include "univ.h"

#include "page0types.h"
#include "fil0fil.h"
#include "buf0buf.h"
#include "data0data.h"
#include "dict0dict.h"
#include "rem0rec.h"
#include "fsp0fsp.h"
#include "mtr0mtr.h"

#ifdef UNIV_MATERIALIZE
#undef UNIV_INLINE
#define UNIV_INLINE
#endif

typedef byte	page_header_t;

/*FIL Header size*/
#define PAGE_HEADER		FSEG_PAGE_DATA

/*PAGE HEADER各种信息偏移*/
#define	PAGE_N_DIR_SLOTS			0		/*page directory拥有的slot个数*/
#define PAGE_HEAP_TOP				2
#define PAGE_N_HEAP					4
#define PAGE_FREE					6
#define PAGE_GARBAGE				8
#define PAGE_LAST_INSERT			10
#define PAGE_DIRECTION				12
#define PAGE_N_DIRECTION			14
#define PAGE_N_RECS					16
#define PAGE_MAX_TRX_ID				18
#define PAGE_HEADER_PRIV_END		26
#define PAGE_LEVEL					28
#define PAGE_BTR_SEG_LEAF			36
#define PAGE_BTR_IBUF_FREE_LIST		PAGE_BTR_SEG_LEAF
#define PAGE_BTR_IBUF_FREE_LIST_NODE PAGE_BTR_SEG_LEAF
#define PAGE_BTR_SEG_TOP			(36 + FSEG_HEADER_SIZE)
#define PAGE_DATA					(PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE)
#define PAGE_INFIMUM				(PAGE_DATA + 1 + REC_N_EXTRA_BYTES)				/*本page中索引最小的记录位置*/
#define PAGE_SUPREMUM				(PAGE_DATA + 2 + 2 * REC_N_EXTRA_BYTES + 8)		/*本page中索引最大的记录位置*/
#define PAGE_SUPREMUM_END			(PAGE_SUPREMUM + 9)								/*本page中索引最大的记录结束的位置偏移*/

/*page的游标运动的方向*/
#define PAGE_LEFT					1
#define PAGE_RIGHT					2
#define PAGE_SAME_REC				3
#define PAGE_SAME_PAGE				4
#define PAGE_NO_DIRECTION			5

/*page 目录*/
typedef byte						page_dir_slot_t;
typedef page_dir_slot_t				page_dir_t;
	
#define PAGE_DIR					FIL_PAGE_DATA_END
#define PAGE_DIR_SLOT_SIZE			2

#define PAGE_EMPTY_DIR_START		(PAGE_DIR + 2 * PAGE_DIR_SLOT_SIZE)

/*目录owned的范围区间是[4, 8]*/
#define PAGE_DIR_SLOT_MAX_N_OWNED	8
#define PAGE_DIR_SLOT_MAX_N_OWNED	4

/**************************************************************/
UNIV_INLINE dulint		page_get_max_trx_id(page_t* page);

void					page_set_max_trx_id(page_t* page, dunlint trx_id);

UNIV_INLINE	void		page_update_max_trx_id(page_t*	page, dulint trx_id);

UNIV_INLINE ulint		page_header_get_field(page_t* page, ulint field);

UNIV_INLINE void		page_header_set_field(page_t* page, ulint field, byte* ptr);

UNIV_INLINE void		page_header_reset_last_insert(page_t*, page, mtr_t* mtr);

UNIV_INLINE	rec_t*		page_get_infimum_rec(page_t* page);

UNIV_INLINE rec_t*		page_get_suremum_rec(page_t* page);

rec_t*					page_get_middle_rec(page_t* page);

UNIV_INLINE int			page_cmp_dtuple_rec_with_match(dtuple_t* dtuple, rec_t* rec, ulint* matched_fields, ulint* matched_bytes);

UNIV_INLINE ulint		page_get_n_rec(page_t* page);

ulint					page_rec_get_n_recs_before(rec_t* rec);

UNIV_INLINE page_dir_slot_t* page_dir_get_nth_slot(page_t* page, ulint n);

UNIV_INLINE ibool		page_rec_check(rec_t* rec);

UNIV_INLINE rec_t*		page_dir_slot_get_rec(page_dir_slot_t* slot);

UNIV_INLINE void		page_dir_slot_set_rec(page_dir_slot_t* slot);

UNIV_INLINE ulint		page_dir_slot_get_n_owned(page_dir_slot_t* slot);

UNIV_INLINE void		page_dir_slot_set_n_owned(page_dir_slot_t* slot, ulint n);

UNIV_INLINE ulint		page_dir_calc_reserved_space(ulint n_recs);

ulint					page_dir_find_owner_slot(rec_t* rec);

UNIV_INLINE rec_t*		page_rec_get_next(rec_t* rec);

UNIV_INLINE void		page_rec_set_next(rec_t* rec, rec_t* next);

UNIV_INLINE ibool		page_rec_is_user_rec(rec_t* rec);

UNIV_INLINE ibool		page_rec_is_infimum(rec_t* rec);

UNIV_INLINE ibool		page_rec_is_supremum(rec_t* rec);

UNIV_INLINE ibool		page_rec_is_first_user_rec(rec_t* rec);

UNIV_INLINE ibool		page_rec_is_last_user_rec(rec_t* rec);

UNIV_INLINE rec_t*		page_rec_find_owner_rec(rec_t* rec);

void					page_rec_write_index_page_no(rec_t* rec, ulint i, ulint page_no, mtr_t* mtr);

UNIV_INLINE ulint		page_get_max_insert_size(page_t* page, ulint n_recs);

UNIV_INLINE ulint		page_get_max_insert_size_after_reorganize(page_t* page, ulint n_recs);

UNIV_INLINE ulint		page_get_free_space_of_empty();

UNIV_INLINE ulint		page_get_data_size(page_t* page);

byte*					page_mem_alloc(page_t* page, ulint need, ulint* heap_no);

UNIV_INLINE void		page_mem_free(page_t* page, rec_t* rec);

page_t*					page_create(buf_frame_t* frame, mtr_t* mtr);

void					page_copy_rec_list_end_no_locks(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr);

void					page_copy_rec_list_end(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr);

void					page_copy_rec_list_start(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr);

void					page_delete_rec_list_end(page_t* page, rec_t* rec, ulint n_recs, ulint size, mtr_t* mtr);

void					page_delete_rec_list_start(page_t* page, rec_t* rec, mtr_t* mtr);

void					page_move_rec_list_end(page_t* new_page, page_t* page, rec_t* split_rec, mtr_t* mtr);

void					page_move_rec_list_start(page_t* new_page, page_t* page, split_rec, mtr_t* mtr);

void					page_dir_split_slot(page_t* page, ulint slot_no);

void					page_dir_balance_slot(page_t* page, ulint slot_no);

byte*					page_parse_delete_rec_list(byte* type, byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

byte*					page_parse_create(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

void					page_rec_print(rec_t* rec);

void					page_dir_print(page_t* page, ulint pr_n);

void					page_print_list(page_t* page, ulint pr_n);

void					page_header_print(page_t* page);

void					page_print(page_t* page, ulint dn, ulint rn);

ibool					page_rec_validate(rec_t* rec);

ibool					page_validate(page_t* page, dict_index_t* index);

rec_t*					page_find_rec_with_heap_no(page_t* page, ulint heap_no);

#include "page0page.inl"

#endif






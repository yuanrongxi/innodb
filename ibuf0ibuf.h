#ifndef __ibuf0ibuf_h_
#define __ibuf0ibuf_h_

#include "univ.h"
#include "dict0mem.h"
#include "dict0dict.h"
#include "mtr0mtr.h"
#include "que0types.h"
#include "ibuf0types.h"
#include "fsp0fsp.h"

#define IBUF_HEADER_PAGE_NO			FSP_IBUF_HEADER_PAGE_NO
#define IBUF_TREE_ROOT_PAGE_NO		FSP_IBUF_TREE_ROOT_PAGE_NO

#define IBUF_HEADER					PAGE_DATA
#define IBUF_TREE_SEG_HEADER		0

ibuf_data_t*				ibuf_data_init_for_space(ulint space);

void						ibuf_init_at_db_start();

void						ibuf_bitmap_page_init(page_t* page, mtr_t* mtr);

void						ibuf_reset_free_bits_with_type(ulint type, page_t* page);

void						ibuf_reset_free_bits(dict_index_t* index);

UNIV_INLINE void			ibuf_update_free_bits_if_full(dict_index_t* index, page_t* page, ulint max_ins_size, ulint increase);

void						ibuf_update_free_bits_low(dict_index_t* index, page_t* page, ulint max_ins_size, mtr_t* mtr);

void						ibuf_update_free_bits_for_two_pages_low(dict_index_t* index, page_t* page1, page_t* page2, mtr_t* mtr);

UNIV_INLINE ibool			ibuf_should_try(dict_index_t* index, ulint ignore_sec_unique);

ibool						ibuf_inside();

UNIV_INLINE ibool			ibuf_bitmap_page(ulint page_no);

ibool						ibuf_page(ulint space, ulint page_no);

ibool						ibuf_page_low(ulint space, ulint page_no, mtr_t* mtr);

ibool						ibuf_index_page_has_free(page_t* page);

void						ibuf_free_excess_pages(ulint space);

ibool						ibuf_insert(dtuple_t* entry, dict_index_t* index, ulint space, ulint page_no, que_thr_t* thr);

void						ibuf_merge_or_delete_for_page(page_t* page, ulint space, ulint page_no);

ulint						ibuf_contract(ibool sync);

ulint						ibuf_contract_for_n_pages(ibool sync, ulint n_pages);

byte*						ibuf_parse_bitmap_init(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

ulint						ibuf_count_get(ulint space, ulint page_no);

void						ibuf_print(char* buf, char* buf_end);

extern ibuf_t* ibuf;

#include "ibuf0ibuf.inl"

#endif





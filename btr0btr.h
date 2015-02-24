#ifndef __btr0btr_h_
#define __btr0btr_h_

#include "univ.h"

#include "dict0dict.h"
#include "data0data.h"
#include "page0cur.h"
#include "rem0rec.h"
#include "mtr0mtr.h"
#include "btr0types.h"

/*一个页能存储的最大记录尺寸（7992，不包括blob类型）*/
#define	BTR_PAGE_MAX_REC_SIZE				(UNIV_PAGE_SIZE / 2 - 200)

/*b-tree的key最大尺寸,1KB*/
#define BTR_PAGE_MAX_KEY_SIZE				1024

/*数据压缩的基准线*/
#define BTR_COMPRESS_LIMIT					(UNIV_PAGE_SIZE / 4 + 1);

/*latch的模式*/
#define BTR_SEARCH_LEAF			RW_S_LATCH
#define BTR_MODIFY_LEAF			RW_X_LATCH
#define BTR_NO_LATCHES			RW_NO_LATCH
#define	BTR_MODIFY_TREE			33
#define	BTR_CONT_MODIFY_TREE	34
#define	BTR_SEARCH_PREV			35
#define	BTR_MODIFY_PREV			36

#define BTR_INSERT				512
#define BTR_ESTIMATE			1024
#define BTR_IGNORE_SEC_UNIQUE	2048

#define BTR_N_LEAF_PAGES 		1
#define BTR_TOTAL_SIZE			2

page_t*							btr_root_get(dict_tree_t* tree, mtr_t* mtr);

UNIV_INLINE page_t*				btr_page_get(ulint space, ulint page_no, ulint mode, mtr_t* mtr);

UNIV_INLINE dulint				btr_page_get_index_id(page_t* page);

UNIV_INLINE void				btr_page_set_index_id(page_t* page, dulint id, mtr_t* mtr);

UNIV_INLINE ulint				btr_page_get_level_low(page_t* page); 

UNIV_INLINE ulint				btr_page_get_next(page_t* page, mtr_t* mtr);

UNIV_INLINE ulint				btr_page_get_prev(page_t* page, mtr_t* mtr);

rec_t*							btr_get_prev_user_rec(rec_t* rec, mtr_t* mtr); 

rec_t*							btr_get_next_user_rec(rec_t* rec, mtr_t* mtr);

UNIV_INLINE void				btr_leaf_page_release(page_t* page, ulint latch_mode, mtr_t* mtr);

UNIV_INLINE ulint				btr_node_ptr_get_child_page_no(rec_t* rec);

ulint							btr_create(ulint type, ulint space, dulint index_id, mtr_t* mtr);

void							btr_free_but_not_root(ulint space, ulint root_page_no);

void							btr_free_root(ulint space, ulint root_page_no, mtr_t* mtr);

rec_t*							btr_root_raise_and_insert(btr_cur_t* cursor, dtuple_t* tuple, mtr_t* mtr);

void							btr_page_reorganize(page_t* page, mtr_t* mtr);

ibool							btr_page_get_split_rec_to_left(btr_cur_t* cursor, rec_t** split_rec);

ibool							btr_page_get_split_rec_to_right(btr_cur_t* cursor, rec_t** split_rec);

rec_t*							btr_page_split_and_insert(btr_cur_t* cursor, dtuple_t* tuple, mtr_t* mtr);

void							btr_insert_on_non_leaf_level(dict_tree_t* tree, ulint level, dtuple_t* tuple, mtr_t* mtr);

void							btr_set_min_rec_mark(rec_t* rec, mtr_t* mtr);

void							btr_node_ptr_delete(dict_tree_t* tree, page_t* page, mtr_t* mtr);

ibool							btr_check_node_ptr(dict_tree_t* tree, page_t* page, mtr_t* mtr);

void							btr_compress(btr_cur_t* cursor, mtr_t* mtr);

void							btr_discard_page(btr_cur_t* cursor, mtr_t* mtr);

byte*							btr_parse_set_min_rec_mark(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

byte*							btr_parse_page_reorganize(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

ulint							btr_get_size(dict_index_t* index, ulint flag);

page_t*							btr_page_alloc(dict_index_t* tree, ulint hint_page_no, byte file_direction, ulint level, mtr_t* mtr);

void							btr_page_free(dict_index_t* tree, page_t* page, mtr_t* mtr);

void							btr_page_free_low(dict_tree_t* tree, page_t* page, ulint level, mtr_t* mtr);

void							btr_print_size(dict_tree_t* tree);

void							btr_print_tree(dict_tree_t* tree, ulint width);

ibool							btr_validate_tree(dict_tree_t* tree);

#include "btr0btr.inl"

#endif



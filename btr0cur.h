/************************************************************************
B+Tree 游标实现
************************************************************************/
#ifndef __btr0cur_h_
#define __btr0cur_h_

#include "univ.h"
#include "dict0dict.h"
#include "data0data.h"
#include "page0cur.h"
#include "btr0types.h"
#include "que0types.h"
#include "row0types.h"
#include "ha0ha.h"

#define BTR_NO_UNDO_LOG_FLAG		1
#define BTR_NO_LOCKING_FLAG			2
#define BTR_KEEP_SYS_FLAG			4

/*自适应HASH索引*/
#define BTR_CUR_ADAPT
#define BTR_CUR_HASH_ADAPT

#define BTR_CUR_HASH				1
#define BTR_CUR_HASH_FAIL			2
#define BTR_CUR_BINARY				3
#define BTR_CUR_INSERT_TO_IBUF		4

#define BTR_CUR_RETRY_DELETE_N_TIMES 100
#define BTR_CUR_RETRY_SLEEP_TIME	50000

#define BTR_EXTERN_SPACE_ID			0
#define BTR_EXTERN_PAGE_NO			4
#define BTR_EXTERN_OFFSET			8
#define BTR_EXTERN_LEN				12

#define BTR_EXTERN_FIELD_REF_SIZE	20

#define BTR_EXTERN_OWNER_FLAG		128
#define BTR_EXTERN_INHERITED_FLAG	64

/*compress page的填充因子，一般是50%*/
#define BTR_CUR_PAGE_COMPRESS_LIMIT	(UNIV_PAGE_SIZE / 2)

#define BTR_PATH_ARRAY_N_SLOTS		250

extern ulint	btr_cur_n_non_sea;
extern ulint	btr_cur_n_sea;
extern ulint	btr_cur_n_non_sea_old;
extern ulint	btr_cur_n_sea_old;

UNIV_INLINE page_cur_t*			btr_cur_get_page_cur(btr_cur_t* cursor);

UNIV_INLINE rec_t*				btr_cur_get_rec(btr_cur_t* cursor);

UNIV_INLINE void				btr_cur_invalidate(btr_cur_t* cursor);

UNIV_INLINE page_t*				btr_cur_get_page(btr_cur_t* cursor);

UNIV_INLINE dict_tree_t*		btr_cur_get_tree(btr_cur_t* cursor);

UNIV_INLINE void				btr_cur_position(dict_index_t* index, rec_t* rec, btr_cur_t* cursor);

void							btr_cur_search_to_nth_level(dict_index_t* index, ulint level, dtuple_t* tuple, ulint mode, ulint latch_mode, 
																btr_cur_t* cursor, ulint has_search_latch, mtr_t* mtr);

void							btr_cur_open_at_index_side(ibool from_left, dict_index_t* index, ulint latch_mode, btr_cur_t* cursor, mtr_t* mtr);

void							btr_cur_open_at_rnd_pos(dict_index_t* index, ulint latch_mode, btr_cur_t* cursor, mtr_t* mtr);

ulint							btr_cur_pessimistic_insert(ulint flags, btr_cur_t* cursor, dtuple_t* entry, rec_t** rec, 
															  big_rec_t** big_rec, que_thr_t* thr, mtr_t* mtr);

ulint							btr_cur_update_sec_rec_in_place(btr_cur_t* cursor, upd_t* update, que_thr_t* thr, mtr_t* mtr);

ulint							btr_cur_update_in_place(ulint flags, btr_cur_t* cursor, upd_t* update, ulint cmpl_info, que_thr_t* thr, mtr_t* mtr);

ulint							btr_cur_optimistic_insert(ulint flags, btr_cur_t* cursor, dtuple_t* entry, rec_t** rec, big_rec_t** big_rec, que_thr_t* thr, mtr_t* mtr);

ulint							btr_cur_optimistic_update(ulint flags, btr_cur_t* cursor, upd_t* update, ulint cmpl_info, que_thr_t* thr, mtr_t* mtr);

ulint							btr_cur_pessimistic_update(ulint flags, btr_cur_t* cursor, big_rec_t** big_rec, upd_t* update, ulint cmpl_info, que_thr_t* thr, mtr_t* mtr);

ulint							btr_cur_del_mark_set_clust_rec(ulint flags, btr_cur_t* cursor, ibool val, que_thr_t* thr, mtr_t* mtr);

ulint							btr_cur_del_mark_set_sec_rec(ulint flags, btr_cur_t* cursor, ibool val, que_thr_t* thr, mtr_t* mtr);

void							btr_cur_del_unmark_for_ibuf(rec_t* rec, mtr_t* mtr);

void							btr_cur_compress(btr_cur_t* cursor, mtr_t* mtr);

ibool							btr_cur_compress_if_useful(btr_cur_t* cursor, mtr_t* mtr);

ibool							btr_cur_optimistic_delete(btr_cur_t* cursor, mtr_t* mtr);

ibool							btr_cur_pessimistic_delete(ulint* err, ibool has_reserved_extents, btr_cur_t* cursor, ibool in_roolback, mtr_t* mtr);

byte*							btr_cur_parse_update_in_place(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

byte*							btr_cur_parse_opt_update(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

byte*							btr_cur_parse_del_mark_set_clust_rec(byte* ptr, byte* end_ptr, page_t* page);

byte*							btr_cur_parse_del_mark_set_sec_rec(byte* ptr, byte* end_ptr, page_t* page);

ib_longlong						btr_estimate_n_rows_in_range(dict_index_t* index, dtuple_t* tuple1, ulint mode1, dtuple_t* tuple2, ulint mode2);

void							btr_estimate_number_of_different_key_vals(dict_index_t* index);

void							btr_cur_mark_extern_inherited_fields(rec_t* rec, upd_t* update, mtr_t* mtr);

void							btr_cur_mark_dtuple_inherited_extern(dtuple_t* entry, ulint* ext_vec, ulint n_ext_vec, upd_t* update);

void							btr_cur_unmark_extern_fields(rec_t* rec, mtr_t* mtr);

void							btr_cur_unmark_dtuple_extern_fields(dtuple_t* entry, ulint* ext_vec, ulint n_ext_vec);

ulint							btr_store_big_rec_extern_fields(dict_index_t* index, rec_t* rec, big_rec_t* big_rec_vec, mtr_t* mtr);

void							btr_free_externally_stored_field(dict_index_t* index, byte* data, ulint local_len, ibool do_not_free_inherited, mtr_t* local_mtr);

void							btr_rec_free_externally_stored_fields(dict_index_t* index, rec_t* rec, ibool do_not_free_inherited, mtr_t* mtr);

byte*							btr_rec_copy_externally_stored_field(rec_t* rec, ulint no, ulint* len, mem_heap_t* heap);

byte*							btr_copy_externally_stored_field(ulint* len, byte* data, ulint local_len, mem_heap_t* heap);

ulint							btr_push_update_extern_fields(ulint* ext_vect, rec_t* rec, upd_t* update);

typedef struct btr_path_struct
{
	ulint		nth_rec;		/*记录在页的位置*/
	ulint		n_recs;			/*这个页拥有的记录数*/
}btr_path_t;

struct btr_cur_struct
{
	dict_index_t*		index;			/*树游标对应的索引对象*/
	page_cur_t			page_cur;		/*树游标对应的page游标*/
	page_t*				left_page;
	que_thr_t			thr;
	ulint				flag;			/* BTR_CUR_HASH, BTR_CUR_HASH_FAIL,BTR_CUR_BINARY, or BTR_CUR_INSERT_TO_IBUF */
	ulint				tree_height;
	ulint				up_match;			
	ulint				up_bytes;
	ulint				low_match;
	ulint				low_bytes;
	ulint				n_fields;
	ulint				n_bytes;
	ulint				fold;			/*对应记录列的fold hash,主要用于hash索引*/
	btr_path_t*			path_arr;		/*path的数组，存有记录的统计信息，用于估算两记录之间存在的记录数*/
};

#include "btr0cur.inl"

#endif





#ifndef __dict0dict_h_
#define __dict0dict_h_

#include "univ.h"
#include "dict0types.h"
#include "dict0mem.h"
#include "data0type.h"
#include "data0data.h"
#include "sync0sync.h"
#include "sync0rw.h"
#include "mem0mem.h"
#include "rem0types.h"
#include "btr0types.h"
#include "ut0mem.h"
#include "ut0lst.h"
#include "hash0hash.h"
#include "ut0rnd.h"
#include "ut0byte.h"
#include "trx0types.h"

extern dict_sys_t*		dict_sys;						/* the dictionary system */
extern rw_lock_t		dict_foreign_key_check_lock;

/*系统数据字典的结构*/
struct dict_sys_struct
{
	mutex_t				mutex;						/*保护系统的数据字典结构的mutex*/
	dulint				row_id;						/*sys row id*/

	hash_table_t*		table_hash;
	hash_table_t*		table_id_hash;
	hash_table_t*		col_hash;
	hash_table_t*		procedure_hash;
	UT_LIST_BASE_NODE_T(dict_table_t) table_LRU;
	ulint				size;

	dict_table_t*		sys_tables;					/* SYS_TABLES table */
	dict_table_t*		sys_columns;				/* SYS_COLUMNS table */
	dict_table_t*		sys_indexes;				/* SYS_INDEXES table */
	dict_table_t*		sys_fields;					/* SYS_FIELDS table */
};

void								dict_table_decrement_handle_count(dict_table_t* table);

void								dict_init();

UNIV_INLINE dict_proc_t*			dict_procedure_get(char* proc_name, trx_t* trx);

void								dict_procedure_add_to_cache(dict_proc_t* proc);

que_t*								dict_procedure_reserve_parsed_copy(dict_proc_t* proc);								

void								dict_procedure_release_parsed_copy(que_t* graph);

UNIV_INLINE dtype_t*				dict_col_get_type(dict_col_t* col);

UNIV_INLINE ulint					dict_col_get_no(dict_col_t* col);

UNIV_INLINE	ulint					dict_col_get_clust_pos(dict_col_t* col);

void								dict_table_autoinc_initialize(dict_table_t* table, ib_longlong value);

ib_longlong							dict_table_autoinc_get(dict_table_t* table);

ib_longlong							dict_table_autoinc_read(dict_table_t* table);

ib_longlong							dict_table_autoinc_peek(dict_table_t* table);

void								dict_table_autoinc_update(dict_table_t* table, ib_longlong value);

void								dict_table_add_to_cache(dict_table_t* table);

void								dict_table_remove_from_cache(dict_table_t* table);

ibool								dict_table_rename_in_cache(dict_table_t* table, char* new_name, ibool rename_also_foreigns);

ulint								dict_foreign_add_to_cache(dict_foreign_t* forgeign);

ulint								dict_create_foreign_constraints(trx_t* trx, char* sql_string, char* name);

dict_table_t*						dict_table_get(char* table_name, trx_t* trx);

dict_table_t*						dict_table_get_and_increment_handle_count(char* table_name, trx_t* trx);

dict_table_t*						dict_table_get_on_id(dulint table_id, trx_t* trx);

UNIV_INLINE dict_table_t*			dict_table_get_on_id_low(dulint table_id, trx_t* trx);

UNIV_INLINE void					dict_table_release(dict_table_t* table);

UNIV_INLINE dict_table_t*			dict_table_check_if_in_cache_low(char* table_name);

UNIV_INLINE	dict_table_t*			dict_table_get_low();

UNIV_INLINE dict_index_t*			dict_table_get_index(dict_table_t* table, char* name);

dict_index_t*						dict_table_get_index_noninline(dict_table_t* table, char* name);

void								dict_table_print(dict_table_t* table);

void								dict_table_print_low(dict_table_t* table);

void								dict_table_print_by_name(char* name);

void								dict_print_info_on_foreign_keys(ibool create_table_format, char* str, ulint len, dict_table_t* table);

UNIV_INLINE	dict_index_t*			dict_table_get_first_index(dict_table_t* table);

dict_index_t*						dict_table_get_first_index_noninline(dict_table_t* table);

UNIV_INLINE ulint					dict_table_get_n_user_cols(dict_table_t* table);

UNIV_INLINE ulint					dict_table_get_n_sys_cols(dict_table_t* table);

UNIV_INLINE ulint					dict_table_get_n_cols(dict_table_t* table);					

UNIV_INLINE dict_col_t*				dict_table_get_nth_col(dict_table_t* table, ulint pos);

UNIV_INLINE dict_col_t*				dict_table_get_sys_col(dict_table_t* table, ulint sys);

UNIV_INLINE ulint					dict_table_get_sys_col_no(dict_table_t* table, ulint sys);

void								dict_table_copy_types(dtuple_t* tuple, dict_table_t* table);

dict_index_t*						dict_index_find_on_id_low(dulint id);

ibool								dict_index_add_to_cache(dict_table_t* table, dict_index_t* index);

UNIV_INLINE ulint					dict_index_get_n_fields(dict_index_t* index);

UNIV_INLINE ulint					dict_index_get_n_unique(dict_index_t* index);

UNIV_INLINE ulint					dict_index_get_n_unique_in_tree(dict_index_t* index);

UNIV_INLINE ulint					dict_index_get_n_ordering_defined_by_user(dict_index_t* index);

UNIV_INLINE dict_field_t*			dict_index_get_nth_field(dict_index_t* index, ulint pos);

UNIV_INLINE dtype_t*				dict_index_get_nth_type(dict_index_t* index, ulint pos);

UNIV_INLINE ulint					dict_index_get_nth_col_no(dict_index_t* index, ulint pos);

ulint								dict_index_get_nth_col_pos(dict_index_t* index, ulint n);

UNIV_INLINE ulint					dict_table_get_nth_col_pos(dict_table_t* table, ulint type);

void								dict_index_copy_types(dtuple_t* tuple, dict_index_t* index, ulint n_fields);

UNIV_INLINE dulint					dict_index_rec_get_sys_col(dict_index_t* index, ulint type, rec_t* rec);

UNIV_INLINE dict_tree_t*			dict_index_get_tree(dict_index_t* index);

UNIV_INLINE dtype_t*				dict_col_get_type(dict_col_t* col);

UNIV_INLINE ulint					dict_field_get_order(dict_field_t* field);

UNIV_INLINE dict_col_t*				dict_field_get_col(dict_field_t* field);

dict_tree_t*						dict_tree_create(dict_index_t* index);

void								dict_tree_free(dict_tree_t* tree);

dict_index_t*						dict_tree_find_index(dict_tree_t* tree, rec_t* rec);

dict_index_t*						dict_tree_find_index_for_tuple(dict_tree_t* tree, dtuple_t* tuple);

UNIV_INLINE ibool					dict_is_mixed_table_rec(dict_table_t* table, rec_t* rec);

dict_index_t*						dict_index_get_if_in_cache(dulint index_id);

ibool								dict_tree_check_search_tuple(dict_tree_t* tree, dtuple_t* tuple);

dtuple_t*							dict_tree_build_node_ptr(dict_tree_t* tree, rec_t* rec, ulint page_no, mem_heap_t* heap, ulint level);

rec_t*								dict_tree_copy_rec_order_prefix(dict_tree_t* tree, rec_t* rec, byte** buf, ulint* buf_size);

dtuple_t*							dict_tree_build_data_tuple(dict_tree_t* tree, rec_t* rec, mem_heap_t* heap);

UNIV_INLINE ulint					dict_tree_get_space(dict_tree_t* tree);

UNIV_INLINE void					dict_tree_set_space(dict_tree_t* tree, ulint space);

UNIV_INLINE ulint					dict_tree_get_page(dict_tree_t* tree);

UNIV_INLINE void					dict_tree_set_page(dict_tree_t* tree, ulint page);

UNIV_INLINE ulint					dict_tree_get_type(dict_tree_t* tree);

UNIV_INLINE rw_lock_t*				dict_tree_get_lock(dict_tree_t* tree);

UNIV_INLINE ulint					dict_tree_get_space_reserve(dict_tree_t* tree);

ulint								dict_index_calc_min_rec_len(dict_index_t* index);

void								dict_update_statistics_low(dict_table_t* table, ibool has_dict_mutex);

void								dict_update_statistics(dict_table_t* table);

void								dict_mutex_enter_for_mysql();

void								dict_mutex_exit_for_mysql();
#endif







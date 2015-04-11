#ifndef __row0upd_h_
#define __row0upd_h_

#include "univ.h"
#include "data0data.h"
#include "btr0types.h"
#include "btr0pcur.h"
#include "dict0types.h"
#include "trx0types.h"
#include "que0types.h"
#include "row0types.h"
#include "pars0types.h"

#define UPD_NODE_MAGIC_N			1579975

/* Update Node execution states */
#define UPD_NODE_SET_IX_LOCK		1
#define UPD_NODE_UPDATE_CLUSTERED	2
#define UPD_NODE_INSERT_CLUSTERED	3
#define UPD_NODE_UPDATE_ALL_SEC		4
#define UPD_NODE_UPDATE_SOME_SEC	5

#define UPD_NODE_NO_ORD_CHANGE		1
#define UPD_NODE_NO_SIZE_CHANGE		2

/*修改列的数据结构*/
struct upd_field_struct
{
	ulint			field_no;
	que_node_t*		exp;
	dfield_t		new_val;
	ibool			extern_storage;
};

/*update的修改列序列的数据结构*/
struct upd_struct
{
	ulint			info_bits;
	ulint			n_fields;
	upd_field_t*	fields;
};

/*update操作任务query graph数据结构*/
struct upd_node_struct
{
	que_common_t			common;
	
	ibool					is_delete;						/* TRUE if delete, FALSE if update */
	ibool					searched_update;				/* TRUE if searched update, FALSE if positioned */
	ibool					select_will_do_update;
	ibool					in_mysql_interface;

	upd_node_t*				cascade_node;
	
	mem_heap_t*				select;

	select_node_t*			select;
	btr_pcur_t*				pcur;

	dict_table_t*			table;
	upd_t*					update;
	ulint					update_n_fields;

	sys_node_list_t			columns;

	ibool					has_clust_rec_x_lock;

	ulint					cmpl_info;

	ulint					state;
	dict_index_t*			index;
	dtuple_t*				row;
	ulint*					ext_vec;
	ulint					n_ext_vec;

	mem_heap_t*				heap;

	sym_node_t*				table_sym;
	que_node_t*				col_assign_list;

	ulint					magic_n;
};


/*构建一个修改列的结构对象*/
UNIV_INLINE	upd_t*			upd_create(ulint n, mem_heap_t* heap);

UNIV_INLINE ulint			update_get_n_fields(upd_t* update);

UNIV_INLINE upd_field_t*	upd_get_nth_field(upd_t* update, ulint n);

UNIV_INLINE void			upd_field_set_field_no(upd_field_t* upd_field, ulint field_no, dict_index_t* index);

byte*						row_upd_write_sys_vals_to_log(dict_index_t* index, trx_t* trx, dulint roll_ptr, byte* log_ptr, mtr_t* mtr);

UNIV_INLINE void			row_upd_rec_sys_fields(rec_t* rec, dict_index_t* index, trx_t* trx, dulint roll_ptr);

void						row_upd_index_entry_sys_field(dtuple_t* entry, dict_index_t* index, ulint type, dulint val);

upd_node_t*					upd_node_create(mem_heap_t* heap);

void						row_upd_index_write_log(upd_t* update, byte* log_ptr, mtr_t* mtr);

ibool						row_upd_changes_field_size(rec_t* rec, dict_index_t* index, upd_t* update);

void						row_upd_rec_in_place(rec_t* rec, upd_t* update);

upd_t*						row_upd_build_sec_rec_difference_binary(dict_index_t* index, dtuple_t* entry, rec_t* rec, mem_heap_t* heap);

upd_t*						row_upd_build_difference_binary(dict_index_t* index, dtuple_t* entry, ulint* ext_vec, ulint n_ext_vec, rec_t* rec, mem_heap_t* heap);

void						row_upd_index_replace_new_col_vals(dtuple_t* entry, dict_index_t* index, upd_t* update);

void						row_upd_clust_index_replace_new_col_vals(dtuple_t* entry, upd_t*update);

ibool						row_upd_changes_ord_field_binary(dtuple_t* row, dict_index_t* index, upd_t* update);

ibool						row_upd_changes_some_index_ord_field_binary(dict_table_t* table, upd_t* update);

que_thr_t*					row_upd_step(que_thr_t* thr);

void						row_upd_in_place_in_select(sel_node_t* sel_node, que_thr_t* thr, mtr_t* mtr);

byte*						row_upd_parse_sys_vals(byte* ptr, byte* end_ptr, ulint* pos, dulint* trx_id, dulint* roll_ptr);

void						row_upd_rec_sys_fields_in_recovery(rec_t* rec, ulint pos, dulint trx_id, dulint roll_ptr);

byte*						row_upd_index_parse(byte* ptr, byte* end_ptr, mem_heap_t* heap, upd_t** update_out);

#include "row0upd.inl"

#endif







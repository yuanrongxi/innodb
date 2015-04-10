#ifndef __row0ins_h_
#define __row0ins_h_

#include "univ.h"
#include "data0data.h"
#include "que0types.h"
#include "dict0types.h"
#include "trx0types.h"
#include "row0types.h"

#define	INS_NODE_MAGIC_N	15849075

/*insert node  type*/
#define INS_SEARCHED		0
#define INS_VALUES			1
#define INS_DIRECT			2

/*insert node执行的状态*/
#define INS_NODE_SET_IX_LOCK	1
#define INS_NODE_ALLOC_ROW_ID	2
#define INS_NODE_INSERT_ENTRIES	3

/*insert操作的query node数据结构*/
struct ins_node_struct
{
	que_common_t			common;
	ulint					ins_type;
	dtuple_t*				row;

	dict_table_t*			table;
	sel_table_t*			select;
	que_node_t*				values_list;
	ulint					state;
	dict_index_t*			index;
	dtuple_t*				entry;

	UT_LIST_BASE_NODE_T(dtuple_t) entry_list;

	byte*					row_id_buf;				/*row id列在row行的缓冲区位置*/
	dulint					trx_id;
	byte*					trx_id_buf;				/* buffer for the trx id sys field in row */

	mem_heap_t*				entry_sys_heap;

	ulint					magic_n;
};

/*插入记录时检查外键约束*/
ulint						row_ins_check_foreign_constraint(ibool check_ref, dict_foreign_t* foreign, dict_table_t* table, 
										dict_index_t* index, dtuple_t* entry, que_thr_t* thr);

/*创建一个insert query node*/
ins_node_t*					ins_node_create(ulint ins_type, dict_table_t* table, mem_heap_t* heap);

void						ins_node_set_new_row(ins_node_t* node, dtuple_t* row);

ulint						row_ins_index_entry_low(ulint mode, dict_index_t* index, dtuple_t* entry, ulint* ext_vec, ulint n_ext_vec, que_thr_t* thr);

ulint						row_ins_index_entry(dict_index_t* index, dtuple_t* entry, ulint n_ext_vec, que_thr_t* thr);

ulint						row_ins(ins_node_t* node, que_thr_t* thr);

que_thr_t*					row_ins_step(que_thr_t* thr);

#endif






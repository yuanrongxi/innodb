#ifndef __dict0crea_h_
#define __dict0crea_h_

#include "univ.h"
#include "dict0types.h"
#include "dict0dict.h"
#include "que0types.h"
#include "row0types.h"
#include "mtr0mtr.h"

/* Table create node states */
#define	TABLE_BUILD_TABLE_DEF	1
#define	TABLE_BUILD_COL_DEF		2
#define	TABLE_COMMIT_WORK		3
#define	TABLE_ADD_TO_CACHE		4
#define	TABLE_COMPLETED			5

/* Index create node states */
#define	INDEX_BUILD_INDEX_DEF	1
#define	INDEX_BUILD_FIELD_DEF	2
#define	INDEX_CREATE_INDEX_TREE	3
#define	INDEX_COMMIT_WORK	4
#define	INDEX_ADD_TO_CACHE	5

struct tab_node_struct
{
	que_common_t			common;
	dict_table_t*			table;
	ins_node_t*				table_ref;
	ins_node_t*				col_ref;
	commit_node_t*			commit_node;
	ulint					state;
	ulint					col_no;
	mem_heap_t*				heap;
};

struct ind_node_struct
{
	que_common_t			common;
	dict_index_t*			index;
	ins_node_t*				ind_def;
	ins_node_t*				field_def;
	commit_node_t*			commit_node;

	ulint					state;
	dict_table_t*			table;
	dtuple_t*				ind_row;
	ulint					field_no;
	mem_heap_t*				heap;
};

void						dict_create_default_index(dict_table_t* table, trx_t* trx);

tab_node_t*					tab_create_graph_create(dict_table_t* table, mem_heap_t* heap);

ind_node_t*					ind_create_graph_create(dict_index_t* index, mem_heap_t* heap);

que_thr_t*					dict_create_table_step(que_thr_t* thr);

que_thr_t*					dict_create_index_step(que_thr_t* thr);

void						dict_drop_index_tree(rec_t* rec, mtr_t* mtr);

ulint						dict_create_or_check_foreign_constraint_tables();

ulint						dict_create_add_foreigns_to_dictionary(dict_table_t* table, trx_t* trx);

#endif





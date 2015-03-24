#include "dict0mem.h"

#include "rem0rec.h"
#include "data0type.h"
#include "mach0data.h"
#include "dict0dict.h"
#include "que0que.h"
#include "pars0pars.h"
#include "lock0lock.h"

#define DICT_HEAP_SIZE		100 /*memory heap size*/

/*创建一个内存表结构对象*/
dict_table_t* dict_mem_table_create(char* name, ulint space, ulint n_cols)
{
	dict_table_t*	table;
	char*			str;
	mem_heap_t*		heap;

	ut_ad(name);

	heap = mem_heap_create(DICT_HEAP_SIZE);
	table = mem_heap_alloc(heap, sizeof(dict_table_t));

	table->heap = heap;
	/*设置表名*/
	str = mem_heap_alloc(heap, 1 + ut_strlen(name));
	ut_strcpy(str, name);

	table->type = DICT_TABLE_ORDINARY;
	table->name = str;
	table->space = space;
	table->n_def = 0;
	table->n_cols = n_cols + DATA_N_SYS_COLS; /*默认的列有row id, roll ptr, trx id,*/
	table->mem_fix = 0;
	table->n_mysql_handles_opened = 0;
	table->n_foreign_key_checks_running = 0;
	table->cached = FALSE;

	table->mix_id = ut_dulint_zero;
	table->mix_len = 0;

	table->cols = mem_heap_alloc(heap, (n_cols + DATA_N_SYS_COLS) * sizeof(dict_col_t));
	UT_LIST_INIT(table->indexes);

	table->auto_inc_lock = mem_heap_alloc(heap, lock_get_size());
	UT_LIST_INIT(table->locks);
	UT_LIST_INIT(table->foreign_list);
	UT_LIST_INIT(table->referenced_list);

	table->does_not_fit_in_memory = FALSE;
	table->stat_initialized = FALSE;
	table->stat_modified_counter = 0;

	mutex_create(&(table->autoinc_mutex));
	mutex_set_level(&(table->autoinc_mutex), SYNC_DICT_AUTOINC_MUTEX);

	table->autoinc_inited = FALSE;
	table->magic_n = DICT_TABLE_MAGIC_N;

	return table;
}

/*创建一个cluster类型的表结构对象*/
dict_table_t* dict_mem_cluster_create(char* name, ulint space, ulint n_cols, ulint mix_len)
{
	dict_table_t* cluster;
	cluster = dict_mem_table_create(name, space, n_cols);
	cluster->type = DICT_TABLE_CLUSTER;
	cluster->mix_len = mix_len;

	return cluster;
}

void dict_mem_table_make_cluster_member(dict_table_t* table, char* cluster_name)
{
	table->type = DICT_TABLE_CLUSTER_MEMBER;
	table->cluster_name = cluster_name;
}

/*向table增加一个column*/
void dict_mem_table_add_col(dict_table_t* table, char* name, ulint mtype, ulint prtype, ulint len, ulint prec)
{
	char*		str;
	dict_col_t*	col;
	dtype_t*	type;

	ut_ad(table && name);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	table->n_def ++;
	col = dict_table_get_nth_col(table, table->n_def - 1);
	/*设置列名*/
	str = mem_heap_alloc(table->heap, 1 + ut_strlen(name));
	ut_strcpy(str, name);
	/*设置列属性*/
	col->ind = table->n_def - 1;
	col->name = str;
	col->table = table;
	col->ord_part = 0;

	col->clust_pos = ULINT_UNDEFINED;
	/*设置列类型*/
	type = dict_col_get_type(col);
	dtype_set(type, mtype, prtype, len, prec);
}

/*创建一个索引对象*/
dict_index_t* dict_mem_index_create(char* table_name, char* index_name, ulint space, ulint type, ulint n_fields)
{
	char*			str;
	dict_index_t*	index;
	mem_heap_t*		heap;

	ut_ad(table_name && index_name);

	heap = mem_heap_create(DICT_HEAP_SIZE);
	index = mem_heap_alloc(heap, sizeof(dict_index_t));

	index->heap = heap;

	str = mem_heap_alloc(heap, 1 + ut_strlen(index_name));
	ut_strcpy(str, index_name);
	/*设置索引属性*/
	index->type = type;
	index->space = space;
	index->name = str;
	index->table_name = table_name;
	index->table = NULL;
	index->n_def = 0;
	index->n_fields = n_fields;
	index->fields = mem_heap_alloc(heap, 1 + n_fields * sizeof(dict_field_t));
	index->stat_n_diff_key_vals = NULL;
	index->cached = FALSE;
	index->magic_n = DICT_INDEX_MAGIC_N;

	return index;
}

/*创建一个外键约束对象*/
dict_foreign_t* dict_mem_foreign_create()
{
	dict_foreign_t*	foreign;
	mem_heap_t*	heap;

	heap = mem_heap_create(DICT_HEAP_SIZE);
	foreign = mem_heap_alloc(heap, sizeof(dict_foreign_t));

	foreign->heap = heap;
	foreign->id = NULL;

	foreign->type = 0;

	foreign->foreign_table = NULL;
	foreign->foreign_table = NULL;
	foreign->foreign_col_names = NULL;

	foreign->referenced_table_name = NULL;
	foreign->referenced_table = NULL;
	foreign->referenced_col_names = NULL;

	foreign->n_fields = 0;

	foreign->foreign_index = NULL;
	foreign->referenced_index = NULL;

	return foreign;
}

/*向索引中增加一列*/
void dict_mem_index_add_field(dict_index_t* index, char* name, ulint order)
{
	dict_field_t* field;

	ut_ad(index && name);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

	index->n_def ++;

	field = dict_index_get_nth_field(index, index->n_def - 1);
	field->name = name;
	field->order = order;
}

/*释放一个索引对象*/
void dict_mem_index_free(dict_index_t* index)
{
	mem_heap_free(index->heap);
}

dict_proc_t*
dict_mem_procedure_create(char*	name, char*	sql_string, que_fork_t*	graph)		/* in: parsed procedure graph */
{
	dict_proc_t*	proc;
	proc_node_t*	proc_node;
	mem_heap_t*	heap;
	char*		str;
	
	ut_ad(name);

	heap = mem_heap_create(128);
	proc = mem_heap_alloc(heap, sizeof(dict_proc_t));
	proc->heap = heap;
	
	str = mem_heap_alloc(heap, 1 + ut_strlen(name));
	ut_strcpy(str, name);
	proc->name = str;

	str = mem_heap_alloc(heap, 1 + ut_strlen(sql_string));
	ut_strcpy(str, sql_string);
	proc->sql_string = str;

	UT_LIST_INIT(proc->graphs);

	proc->mem_fix = 0;

	proc_node = que_fork_get_child(graph);
	proc_node->dict_proc = proc;

	return proc;
}

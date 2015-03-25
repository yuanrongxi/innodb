#include "dict0dict.h"
#include "buf0buf.h"
#include "data0type.h"
#include "mach0data.h"
#include "dict0boot.h"
#include "dict0mem.h"
#include "dict0crea.h"
#include "trx0undo.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0sea.h"
#include "pars0pars.h"
#include "pars0sym.h"
#include "que0que.h"
#include "rem0cmp.h"


dict_sys_t*		dict_sys = NULL; /*innodb的数据字典管理对象*/

rw_lock_t		dict_foreign_key_check_lock;

#define DICT_HEAP_SIZE					100			/*memory heap size*/
#define DICT_POOL_PER_PROCEDURE_HASH	512			
#define DICT_POOL_PER_TABLE_HASH		512
#define DICT_POOL_PER_COL_HASH			128
#define DICT_POOL_PER_VARYING			4

/********************************************************************************************/
/*增加一个column对象到对应的table的数据字典中*/
static void dict_col_add_to_cache(dict_table_t* table, dict_col_t* col);

static void dict_col_reposition_in_cache(dict_table_t* table, dict_col_t* col, char* new_name);

static void dict_col_remove_from_cache(dict_table_t* table, dict_col_t* col);

static void dict_index_remove_from_cache(dict_table_t* table, dict_index_t* index);

UNIV_INLINE void dict_index_add_col(dict_index_t* index, dict_col_t* col, ulint order);

static void dict_index_copy(dict_index_t* index1, dict_index_t* index2, ulint start, ulint end);

static ibool dict_index_find_cols(dict_table_t* table, dict_index_t* index);

static dict_index_t* dict_index_build_internal_clust(dict_table_t* table, dict_index_t* index);

static dict_index_t* dict_index_build_internal_non_clust(dict_table_t* table, dict_index_t* index);

UNIV_INLINE dict_index_t* dict_tree_find_index_low(dict_tree_t* tree, rec_t* rec);

static void dict_foreign_remove_from_cache(dict_foreign_t* foreign);

static void dict_col_print_low(dict_col_t* col);

static void dict_index_print_low(dict_index_t* index);

static void dict_field_print_low(dict_field_t* field);

static void dict_foreign_free(dict_foreign_t* foreign);

/**********************************************************************************************/
#define LOCK_DICT() mutex_enter(&(dict_sys->mutex))
#define UNLOCK_DICT() mutex_exit(&(dict_sys->mutex));

/*对dict_sys->mutex加锁*/
void dict_mutex_enter_for_mysql()
{
	LOCK_DICT();
}

/*对dict_sys->mutex释放锁*/
void dict_mutex_exit_for_mysql()
{
	UNLOCK_DICT();
}

/*对table->n_mysql_handles_opened计数器-1*/
void dict_table_decrement_handle_count(dict_table_t* table)
{
	LOCK_DICT();

	ut_a(table->n_mysql_handles_opened > 0);
	table->n_mysql_handles_opened --;

	UNLOCK_DICT();
}

/*获得表的第N列的column对象*/
dict_col_t* dict_table_get_nth_col_noninline(dict_table_t* table, ulint pos)
{
	return dict_table_get_nth_col(table, pos);
}

dict_index_t* dict_table_get_first_index_noninline(dict_table_t* table)
{
	return(dict_table_get_first_index(table));
}

dict_index_t* dict_table_get_next_index_noninline(dict_index_t* index)
{
	return dict_table_get_next_index(index);
}

dict_index_t* dict_table_get_index_noninline(dict_table_t* table, char* name)
{
	return dict_table_get_index(table, name);
}

/*初始化autoinc的计数器*/
void dict_table_autoinc_initialize(dict_table_t* table, ib_longlong value)
{
	mutex_enter(&(table->autoinc_mutex));

	table->autoinc_inited = TRUE;
	table->autoinc = value;

	mutex_exit(&(table->autoinc_mutex));
}

/*获得一个新的自增ID值*/
ib_longlong dict_table_autoinc_get(dict_table_t* table)
{
	ib_longlong value;

	mutex_enter(&(table->autoinc_mutex));
	if(!table->autoinc_inited) /*autoinc未初始化*/
		value = 0;
	else{
		value = table->autoinc;
		table->autoinc = table->autoinc + 1; 
	}
	mutex_exit(&(table->autoinc_mutex));

	return value;
}

/*对表的autoinc的读取，不自增*/
ib_longlong dict_table_autoinc_read(dict_table_t* table)
{
	ib_longlong value;

	mutex_enter(&(table->autoinc_mutex));
	if(!table->autoinc_inited)
		value = 0;
	else
		value = table->autoinc;
	mutex_exit(&(table->autoinc_mutex));

	return value;
}

/*对table自增ID的读取，不加latch保护*/
ib_longlong dict_table_autoinc_peek(dict_table_t* table)
{
	ib_longlong value;

	if(!table->autoinc_inited)
		value = 0;
	else
		value = table->autoinc;

	return value;
}

/*对table自增ID的更新*/
void dict_table_autoinc_update(dict_table_t* table, ib_longlong value)
{
	mutex_enter(&(table->autoinc_mutex));

	if(table->autoinc_inited){
		if(value >= table->autoinc)
			table->autoinc = value + 1;
	}

	mutex_exit(&(table->autoinc_mutex));
}

/*查找索引对象对应的表中的第N个column的pos*/
ulint dict_index_get_nth_col_pos(dict_index_t* index, ulint n)
{
	dict_field_t*	field;
	dict_col_t*	col;
	ulint		pos;
	ulint		n_fields;

	if(index->type & DICT_CLUSTERED){
		col = dict_table_get_nth_col(index->table, n);
		return col->clust_pos;
	}

	n_fields = dict_index_get_n_fields(index);
	for(pos = 0; pos < n_fields; pos ++){
		field = dict_index_get_nth_field(index, pos);
		col = field->col;
		if(dict_col_get_no(col) == n)
			return pos;
	}

	return ULINT_UNDEFINED;
}

/*通过table id和trx获得表的数据字典*/
dict_table_t* dict_table_get_on_id(dulint table_id, trx_t* trx)
{
	dict_table_t* table;

	if(ut_dulint_cmp(table_id, DICT_FIELDS_ID) <= 0 || trx->dict_operation){ /*DDL操作事务或者系统表已经持有dict_sys->mutex*/
		ut_ad(mutex_own(&(dict_sys->mutex)));
		return dict_table_get_on_id_low(table_id, trx);
	}

	LOCK_DICT();
	table = dict_table_get_on_id_low(table_id, trx);
	UNLOCK_DICT();

	return table;
}
/*查找聚集索引中第n个column的pos*/
ulint dict_table_get_nth_col_pos(dict_table_t* table, ulint n)
{
	/*第一个索引就是cluster index*/
	return dict_index_get_nth_col_pos(dict_table_get_first_index(table), n);
}

/*创建dict_sys管理对象*/
void dict_init()
{
	dict_sys = mem_alloc(sizeof(dict_sys_t));

	mutex_create(&(dict_sys->mutex));
	mutex_set_level(&(dict_sys->mutex), SYNC_DICT);

	/*根据buffer pool的最大值创建各个数据字典的hash表*/
	dict_sys->table_hash = hash_create(buf_pool_get_max_size() / (DICT_POOL_PER_TABLE_HASH * UNIV_WORD_SIZE));
	dict_sys->table_id_hash = hash_create(buf_pool_get_max_size() / (DICT_POOL_PER_TABLE_HASH *UNIV_WORD_SIZE));
	dict_sys->col_hash = hash_create(buf_pool_get_max_size() / (DICT_POOL_PER_COL_HASH * UNIV_WORD_SIZE));
	dict_sys->procedure_hash = hash_create(buf_pool_get_max_size() / (DICT_POOL_PER_PROCEDURE_HASH * UNIV_WORD_SIZE));

	dict_sys->size = 0;
	UT_LIST_INIT(dict_sys->table_LRU);

	/*创建外键约束保护latch*/
	rw_lock_create(&dict_foreign_key_check_lock);
	rw_lock_set_level(&dict_foreign_key_check_lock, SYNC_FOREIGN_KEY_CHECK);
}

/*通过表名获得表的数据字典*/
dict_table_t* dict_table_get(char* table_name, trx_t* trx)
{
	dict_table_t*	table;

	UT_NOT_USED(trx);

	LOCK_DICT();
	table=  dict_table_get_low(table_name);
	UNLOCK_DICT();

	if(table != NULL && !table->stat_initialized){
		dict_update_statistics(table);
	}

	return table;
}

/*对MSYQL open handled加1，并返回对应的表数据字典对象*/
dict_table_t* dict_table_get_and_increment_handle_count(char* table_name, trx_t* trx)
{
	dict_table_t* table;

	UT_NOT_USED(trx);
	LOCK_DICT();

	table = dict_table_get_low(table_name);
	if(table != NULL)
		table->n_mysql_handles_opened ++;

	UNLOCK_DICT();

	if(table != NULL && !table->stat_initialized)
		dict_update_statistics(table);

	return table;
}

/*将table数据字典加入到字典cache当中*/
void dict_table_add_to_cache(dict_table_t* table)
{
	ulint	fold;
	ulint	id_fold;
	ulint	i;

	ut_ad(table);
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(table->n_def == table->n_cols - DATA_N_SYS_COLS);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
	ut_ad(table->cached == FALSE);

	/*计算名字哈希值和id哈希值*/
	fold = ut_fold_string(table->name);
	id_fold = ut_fold_dulint(table->id);

	table->cached = TRUE;

	/*增加默认的系统column(row id, trx id, roll_ptr, mix id)*/
	dict_mem_table_add_col(table, "DB_ROW_ID", DATA_SYS, DATA_ROW_ID, 0, 0);
	dict_mem_table_add_col(table, "DB_TRX_ID", DATA_SYS, DATA_TRX_ID, 0, 0);
	dict_mem_table_add_col(table, "DB_ROLL_PTR", DATA_SYS, DATA_ROLL_PTR, 0, 0);
	dict_mem_table_add_col(table, "DB_MIX_ID", DATA_SYS, DATA_MIX_ID, 0, 0);

	/*对以名字作为哈希在table_hash中查找，一定是没有的，如果有，会发生内存泄露？*/
	{
		dict_table_t*	table2;
		HASH_SEARCH(name_hash, dict_sys->table_hash, fold, table2, (ut_strcmp(table2->name, table->name) == 0));
		ut_ad(table2 == NULL);
	}

	{
		dict_table_t*	table2;
		HASH_SEARCH(id_hash, dict_sys->table_id_hash, id_fold, table2, (ut_dulint_cmp(table2->id, table->id) == 0));
		ut_a(table2 == NULL);
	}

	if(table->type == DICT_TABLE_CLUSTER_MEMBER){
		table->mix_id_len = mach_dulint_get_compressed_size(table->mix_id);
		mach_dulint_write_compressed(table->mix_id_buf, table->mix_id);
	}

	/*将table中所有的column对象写入到cache中*/
	for(i = 0; i < table->n_cols; i ++)
		dict_col_add_to_cache(table, dict_table_get_nth_col(table, i));

	/*以名字哈希隐射关系插入table到cache中*/
	HASH_INSERT(dict_table_t, name_hash, dict_sys->table_hash, fold, table);
	/*以ID哈希隐射关系插入table到cache中*/
	HASH_INSERT(dict_table_t, id_hash, dict_sys->table_id_hash, id_fold, table);

	/*将table加入到dict_sys的LRU淘汰列表中，如果时间太长没有引用，应该会从cache中删除table*/
	UT_LIST_ADD_FIRST(table_LRU, dict_sys->table_LRU, table);

	dict_sys->size += mem_heap_get_size(table->heap);
}

/*通过index id在内存table lru cache中找到对应的索引对象,整体扫描table LRU所有的表数据字典*/
dict_index_t* dict_index_find_on_id_low(dulint id)
{
	dict_table_t*	table;
	dict_index_t*	index;

	table = UT_LIST_GET_FIRST(dict_sys->table_LRU);
	while(table != NULL){
		index = dict_table_get_first_index(table);
		while(index != NULL){
			if(0 == ut_dulint_cmp(id, index->tree->id))
				return index;

			index = dict_table_get_next_index(index);
		}

		table = UT_LIST_GET_NEXT(table_LRU, table);
	}

	return index;
}

/*对表进行改名，如果表在cache中，cache中的表数据字典做相对应的更新*/
ibool dict_table_rename_in_cache(dict_table_t* table, char* new_name, ibool rename_also_foreigns)
{
	dict_foreign_t*	foreign;
	dict_index_t*	index;
	ulint			fold;
	ulint			old_size;
	char*			name_buf;
	ulint			i;

	ut_ad(table);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	old_size = mem_heap_get_size(table->heap);
	fold = ut_fold_string(new_name);

	{
		dict_table_t*	table2;
		HASH_SEARCH(name_hash, dict_sys->table_hash, fold, table2,
			(ut_strcmp(table2->name, new_name) == 0));
		if (table2)
			return(FALSE);
	}

	/*对column对象做表名字更新*/
	for(i = 0; i < table->n_cols; i ++)
		dict_col_reposition_in_cache(table, dict_table_get_nth_col(table, i), new_name);

	/*删除原来的对应关系*/
	HASH_DELETE(dict_table_t, name_hash, dict_sys->table_hash, ut_fold_string(table->name), table);
	name_buf = mem_heap_alloc(table->heap, ut_strlen(new_name) + 1);
	ut_memcpy(name_buf, new_name, ut_strlen(new_name) + 1);
	table->name = name_buf;
	/*重新插入到table hash中*/
	HASH_INSERT(dict_table_t, name_hash, dict_sys->table_hash, fold, table);
	/*更新数据字典占用的空间*/
	dict_sys->size += (mem_heap_get_size(table->heap) - old_size);

	/*更新索引对应的表名*/
	index = dict_table_get_first_index(table);
	while(index != NULL){
		index->table_name = table->name;
		index = dict_table_get_next_index(index);
	}
	
	/*不更新外键约束关系，那么将取消对应的外键约束*/
	if(!rename_also_foreigns){
		foreign = UT_LIST_GET_LAST(table->foreign_list);
		while(foreign != NULL){
			dict_foreign_remove_from_cache(foreign);
			foreign = UT_LIST_GET_LAST(table->foreign_list);
		}

		foreign = UT_LIST_GET_FIRST(table->referenced_list);
		while(foreign != NULL){
			foreign->referenced_table = NULL;
			foreign->referenced_index = NULL;
			foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
		}

		UT_LIST_INIT(table->referenced_list);
		return TRUE;
	}

	/*需要对外键约束更新*/
	foreign = UT_LIST_GET_FIRST(table->foreign_list);

	while (foreign != NULL) {
		if (ut_strlen(foreign->foreign_table_name) < ut_strlen(table->name)) {
			foreign->foreign_table_name = mem_heap_alloc(foreign->heap, ut_strlen(table->name) + 1);
		}

		ut_memcpy(foreign->foreign_table_name, table->name, ut_strlen(table->name) + 1);
		foreign->foreign_table_name[ut_strlen(table->name)] = '\0';

		foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
	}

	foreign = UT_LIST_GET_FIRST(table->referenced_list);

	while (foreign != NULL) {
		if(ut_strlen(foreign->referenced_table_name) < ut_strlen(table->name))
			foreign->referenced_table_name = mem_heap_alloc(foreign->heap,ut_strlen(table->name) + 1);

		ut_memcpy(foreign->referenced_table_name, table->name, ut_strlen(table->name) + 1);
		foreign->referenced_table_name[ut_strlen(table->name)] = '\0';

		foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
	}

	return TRUE;
}

/*从系统字典cache中删除掉指定的table数据字典对象*/
void dict_table_remove_from_cache(dict_table_t* table)
{
	dict_foreign_t*	foreign;
	dict_index_t*	index;
	ulint		size;
	ulint		i;

	ut_ad(table);
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	/*先删除外键约束cache*/
	foreign = UT_LIST_GET_LAST(table->foreign_list);
	while(foreign != NULL){
		dict_foreign_remove_from_cache(foreign);
		foreign = UT_LIST_GET_LAST(table->foreign_list);
	}

	foreign = UT_LIST_GET_FIRST(table->referenced_list);
	while(foreign != NULL){
		foreign->referenced_table = NULL;
		foreign->referenced_index = NULL;
		foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
	}

	/*删除索引对象的cache*/
	index = UT_LIST_GET_LAST(table->indexes);
	while(index != NULL){
		dict_index_remove_from_cache(table, index);
		index = UT_LIST_GET_LAST(table->indexes);
	}

	/*删除column的cache*/
	for(i = 0; i < table->n_cols; i ++)
		dict_col_remove_from_cache(table, dict_table_get_nth_col(table, i));

	/*从hash表中删除table的对应关系*/
	HASH_DELETE(dict_table_t, name_hash, dict_sys->table_hash, ut_fold_string(table->name), table);
	HASH_DELETE(dict_table_t, id_hash, dict_sys->table_id_hash, ut_fold_dulint(table->id), table);

	/*从table LRU中删除前后关系*/
	UT_LIST_REMOVE(table_LRU, dict_sys->table_LRU, table);
	/*free 掉ID自增长保护锁*/
	mutex_free(&(table->autoinc_mutex));

	/*更新dict_sys的空间占用统计*/
	size = mem_heap_get_size(table->heap);
	ut_ad(dict_sys->size >= size);
	dict_sys->size -= size;
	/*释放掉table的堆*/
	mem_heap_free(table->heap);
}

/*dict_sys cache了太多的表数据字典，需要进行LRU淘汰，从table_LRU的末尾开始淘汰*/
void dict_table_LRU_trim()
{
	dict_table_t*	table;
	dict_table_t*	prev_table;

	ut_a(0);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	table = UT_LIST_GET_LAST(dict_sys->table_LRU);
	while(table != NULL && dict_sys->size > buf_pool_get_max_size() / DICT_POOL_PER_VARYING){ /*占用了buffer pool的1/4以上就会淘汰*/
		prev_table = UT_LIST_GET_PREV(table_LRU, table);
		if(table->mem_fix == 0) /*只淘汰没有被外部fix的表数据字典*/
			dict_table_remove_from_cache(table);
		table = prev_table;
	}
}

/*增加一个column到数据字典的cache*/
static void dict_col_add_to_cache(dict_table_t* table, dict_col_t* col)
{
	ulint	fold;

	ut_ad(table && col);
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	fold = ut_fold_ulint_pair(ut_fold_string(table->name), ut_fold_string(col->name));
	{ /*column name应该不存在与col hash当中*/
		dict_col_t*	col2;
		HASH_SEARCH(hash, dict_sys->col_hash, fold, col2, (ut_strcmp(col->name, col2->name) == 0) && (ut_strcmp((col2->table)->name, table->name) == 0));  
		ut_a(col2 == NULL);
	}

	HASH_INSERT(dict_col_t, hash, dict_sys->col_hash, fold, col);
}

/*从数据字典cache中删除一个column对象*/
static void dict_col_remove_from_cache(dict_table_t* table, dict_col_t* col)
{
	ulint		fold;

	ut_ad(table && col);
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	fold = ut_fold_ulint_pair(ut_fold_string(table->name), ut_fold_string(col->name));
	HASH_DELETE(dict_col_t, hash, dict_sys->col_hash, fold, col);
}

/*替换column对应的表名字*/
static void dict_col_reposition_in_cache(dict_table_t* table, dict_col_t* col, char* new_name)
{
	ulint fold;

	ut_ad(table && col);
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	fold = ut_fold_ulint_pair(ut_fold_string(table->name), ut_fold_string(col->name));
	HASH_DELETE(dict_col_t, hash, dict_sys->col_hash, fold, col);

	/*替换成新的名字做隐射关系*/
	fold = ut_fold_ulint_pair(ut_fold_string(new_name), ut_fold_string(col->name));
	HASH_INSERT(dict_col_t, hash, dict_sys->col_hash, fold, col);
}

/*在表table中增加一个索引对象，并放入数据字典cache中*/
ibool dict_index_add_to_cache(dict_table_t* table, dict_index_t* index)
{
	dict_index_t*	new_index;
	dict_tree_t*	tree;
	dict_table_t*	cluster;
	dict_field_t*	field;
	ulint		n_ord;
	ibool		success;
	ulint		i;
	ulint		j;

	ut_ad(index);
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(index->n_def == index->n_fields);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

	ut_ad(mem_heap_validate(index->heap));
	/*检查是否索引重复*/
	{
		dict_index_t* index2 = UT_LIST_GET_FIRST(table->indexes);
		while(index2 != NULL){
			ut_ad(ut_strcmp(index->name, index2->name) != 0);
			index2 = UT_LIST_GET_NEXT(indexes, index2);
		}

		ut_a(UT_LIST_GET_LEN(table->indexes) == 0 || (index->type & DICT_CLUSTERED) == 0);
	}

	/*检查表数据字典中是否有index建索引的列*/
	success = dict_index_find_cols(table, index);
	if(!success){ /*没有支持新加索引的列*/
		dict_mem_index_free(index);
		return FALSE;
	}

	for(i = 0; i < dict_index_get_nth_field(index); i ++){
		for (j = 0; j < i; j++) {
			if(dict_index_get_nth_field(index, j)->col == dict_index_get_nth_field(index, i)->col){ /*索引列重复*/
				fprintf(stderr,
					"InnoDB: Error: column %s appears twice in index %s of table %s\n"
					"InnoDB: This is not allowed in InnoDB.\n"
					"InnoDB: UPDATE can cause such an index to become corrupt in InnoDB.\n",
					dict_index_get_nth_field(index, i)->col->name, index->name, table->name);
			}
		}
	}

	/*调整索引,得到一个内部新的索引对象*/
	if(index->type & DICT_CLUSTERED)
		new_index = dict_index_build_internal_clust(table, index);
	else
		new_index = dict_index_build_internal_non_clust(table, index);
	/*构建一个自适应HASH*/
	new_index->search_info = btr_search_info_create(new_index->heap);
	new_index->n_fields = new_index->n_def;

	UT_LIST_ADD_LAST(indexes, table->indexes, new_index);
	new_index->table = table;
	new_index->table_name = table->name;

	if(index->type & DICT_UNIVERSAL)
		n_ord = new_index->n_fields;
	else
		n_ord = dict_index_get_n_unique(new_index);
	/*更新column索引排序的次数*/
	for(i = 0; i < n_ord; i ++){
		field = dict_index_get_nth_field(new_index, i);
		dict_field_get_col(field)->ord_part ++;
	}

	/*构建索引树对象*/
	if(table->type == DICT_TABLE_CLUSTER_MEMBER){ /*聚集索引的成员,直接用聚集索引树对象*/
		cluster = dict_table_get_low(table->cluster_name);
		tree = dict_index_get_tree(UT_LIST_GET_FIRST(cluster->indexes));

		new_index->tree = tree;
		new_index->page_no = tree->page;
	}
	else{
		tree = dict_tree_create(new_index);
		ut_a(tree);
		new_index->tree = tree;
	}

	if(!(new_index->type & DICT_UNIVERSAL)){
		new_index->stat_n_diff_key_vals = mem_heap_alloc(new_index->heap, (1 + dict_index_get_n_unique(new_index)) * sizeof(ib_longlong));
		for(i = 0; i < dict_index_get_n_unique(new_index); i++)
			new_index->stat_n_diff_key_vals[i] = 100;
	}

	UT_LIST_ADD_LAST(tree_indexes, tree->tree_indexes, new_index);

	dict_sys->size += mem_heap_get_size(new_index->heap);
	dict_mem_index_free(index);

	return TRUE;
}

/*将index从数据字典cache中删除*/
static void dict_index_remove_from_cache(dict_table_t* table, dict_index_t* index)
{
	dict_field_t*	field;
	ulint		size;
	ulint		i;

	ut_ad(table && index);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	ut_ad(UT_LIST_GET_LEN((index->tree)->tree_indexes) == 1);
	
	/*释放索引树对象*/
	dict_tree_free(index->tree);

	/*修改column的ord_part次数*/
	for(i = 0; i < dict_index_get_n_unique(index); i++){
		field = dict_index_get_nth_field(index, i);
		ut_ad(dict_field_get_col(field)->ord_part > 0);
		dict_field_get_col(field)->ord_part --;
	}
	/*从indexes中删除*/
	UT_LIST_REMOVE(indexes, table->indexes, index);

	/*更新dict_sys->size*/
	size = mem_heap_get_size(index->heap);
	ut_ad(dict_sys->size >= size);
	dict_sys->size -= size;
	/*释放索引对象内存*/
	mem_heap_free(index->heap);
}

/*检查table中是否有构建index所用的列*/
static ibool dict_index_find_cols(dict_table_t* table, dict_index_t* index)
{
	dict_col_t*	col;
	dict_field_t*	field;
	ulint		fold;
	ulint		i;

	ut_ad(table && index);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	for(i = 0; i < index->n_fields; i++){
		field = dict_index_get_nth_field(index, i);
		fold = ut_fold_ulint_pair(ut_fold_string(table->name), ut_fold_string(field->name));

		HASH_SEARCH(hash, dict_sys->col_hash, fold, col, (ut_strcmp(col->name, field->name) == 0) 
			&& (ut_strcmp((col->table)->name, table->name) == 0)); 
		if(col == NULL)
			return FALSE;
		else
			field->col = col;
	}

	return TRUE;
}

/*向索引对象中添加一列*/
UNIV_INLINE void dict_index_add_col(dict_index_t* index, dict_col_t* col, ulint order)
{
	dict_field_t*	field;
	dict_mem_index_add_field(index, col->name, order);
	field = dict_index_get_nth_field(index, index->n_def - 1);

	field->col = col;
}

/*将index2中的fields拷贝到index1中*/
static void dict_index_copy(dict_index_t* index1, dict_index_t* index2, ulint start, ulint end)
{
	dict_field_t*	field;
	ulint			i;

	for(i = start; i < end; i++){
		field = dict_index_get_nth_field(index2, i);
		dict_index_add_col(index1, field->col, field->order);
	}
}

/*将index中fields的type拷贝到tuple中*/
void dict_index_copy_types(dtuple_t* tuple, dict_index_t* index, ulint n_fields)
{
	dtype_t*	dfield_type;
	dtype_t*	type;
	ulint		i;

	if(index->type & DICT_UNIVERSAL){
		dtuple_set_types_binary(tuple, n_fields);
		return ;
	}

	for (i = 0; i < n_fields; i++) {
		dfield_type = dfield_get_type(dtuple_get_nth_field(tuple, i));
		type = dict_col_get_type(dict_field_get_col( dict_index_get_nth_field(index, i)));
		*dfield_type = *type;
	}
}

/*将table中的column type拷贝到tuple中*/
void dict_table_copy_types(dtuple_t* tuple, dict_table_t* table)
{
	dtype_t*	dfield_type;
	dtype_t*	type;
	ulint		i;

	ut_ad(!(table->type & DICT_UNIVERSAL));
	for(i = 0; i < dtuple_get_n_fields(tuple); i ++){
		dfield_type = dfield_get_type(dtuple_get_nth_field(tuple, i));
		type = dict_col_get_type(dict_table_get_nth_col(table, i));

		*dfield_type = *type;
	}
}

/*构建一个数据字典内部的聚集索引对象*/
static dict_index_t* dict_index_build_internal_clust(dict_table_t* table, dict_index_t* index)
{
	dict_index_t*	new_index;
	dict_field_t*	field;
	dict_col_t*	col;
	ulint		fixed_size;
	ulint		trx_id_pos;
	ulint		i;

	ut_ad(table && index);
	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	/*构建一个内存索引对象*/
	new_index = dict_mem_index_create(table->name, index->name, table->space, index->type, index->n_fields + table->n_cols);
	/*设置索引用到的列数*/
	new_index->n_user_defined_cols = index->n_fields;
	/*设置索引ID和root page no*/
	new_index->id = index->id;
	new_index->page_no = index->page_no;

	if(table->type != DICT_TABLE_ORDINARY){
		/*拷贝索引的fields*/
		dict_index_copy(new_index, index, 0, table->mix_len);
		/*增加一个mix id column*/
		dict_index_add_col(new_index, dict_table_get_sys_col(table, DATA_MIX_ID), 0);
		dict_index_copy(new_index, index, table->mix_len, index->n_fields);
	}
	else
		dict_index_copy(new_index, index, 0, index->n_fields);

	if(index->type & DICT_UNIVERSAL)
		new_index->n_uniq = ULINT_MAX;
	else if(index->type & DICT_UNIQUE)
		new_index->n_uniq = new_index->n_def;
	else
		new_index->n_uniq = 1 + new_index->n_def;

	new_index->trx_id_offset = 0;

	
	if(!(index->type & DICT_IBUF)){
		/* Add system columns, trx id first */
		trx_id_pos = new_index->n_def;

		if (!(index->type & DICT_UNIQUE)) {
			dict_index_add_col(new_index, dict_table_get_sys_col(table, DATA_ROW_ID), 0);
			trx_id_pos++;
		}

		dict_index_add_col(new_index, dict_table_get_sys_col(table, DATA_TRX_ID), 0);	
		dict_index_add_col(new_index, dict_table_get_sys_col(table, DATA_ROLL_PTR), 0);

		for(i = 0; i < trx_id_pos; i ++){
			fixed_size = dtype_get_fixed_size(dict_index_get_nth_type(new_index, i));
			if(fixed_size == 0){
				new_index->trx_id_offset = 0;
				break;
			}

			new_index->trx_id_offset += fixed_size;
		}
	}

	for (i = 0; i < table->n_cols; i++) {
		col = dict_table_get_nth_col(table, i);
		col->aux = ULINT_UNDEFINED;
	}

	for(i = 0; i < new_index->n_def; i++){
		field = dict_index_get_nth_field(new_index, i);
		field->col->aux = 0;
	}

	for(i = 0; i < table->n_cols - DATA_N_SYS_COLS; i ++){
		col = dict_table_get_nth_col(table, i);
		ut_ad(col->type.mtype != DATA_SYS);

		if (col->aux == ULINT_UNDEFINED)
			dict_index_add_col(new_index, col, 0);
	}

	ut_ad((index->type & DICT_IBUF) || (UT_LIST_GET_LEN(table->indexes) == 0));
	for(i = 0; i < new_index->n_def; i ++){
		field = dict_index_get_nth_field(new_index, i);
		(field->col)->clust_pos = i;
	}

	new_index->cached = TRUE;

	return new_index;
}

static dict_index_t* dict_index_build_internal_non_clust(dict_table_t* table, dict_index_t* index)
{
	dict_field_t*	field;
	dict_index_t*	new_index;
	dict_index_t*	clust_index;
	ulint		i;

	ut_ad(table && index);
	ut_ad(0 == (index->type & DICT_CLUSTERED));
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	clust_index = UT_LIST_GET_FIRST(table->indexes);
	ut_ad(clust_index);
	ut_ad(clust_index->type & DICT_CLUSTERED);
	ut_ad(!(clust_index->type & DICT_UNIVERSAL));

	new_index = dict_mem_index_create(table->name, index->name, index->space, index->type, index->n_fields + 1 + clust_index->n_uniq);
	new_index->n_user_defined_cols = index->n_fields;
	new_index->id = index->id;
	new_index->page_no = index->page_no;

	dict_index_copy(new_index, index, 0, index->n_fields);

	for (i = 0; i < clust_index->n_uniq; i++) {
		field = dict_index_get_nth_field(clust_index, i);
		(field->col)->aux = ULINT_UNDEFINED;
	}

	for(i = 0; i < new_index->n_def; i ++){
		field = dict_index_get_nth_field(new_index, i);
		(field->col)->aux = 0;
	}

	for (i = 0; i < clust_index->n_uniq; i++) {
		field = dict_index_get_nth_field(clust_index, i);
		if ((field->col)->aux == ULINT_UNDEFINED)
			dict_index_add_col(new_index, field->col, 0);
	}

	if ((index->type) & DICT_UNIQUE)
		new_index->n_uniq = index->n_fields;
	else
		new_index->n_uniq = new_index->n_def;

	new_index->n_fields = new_index->n_def;
	new_index->cached = TRUE;

	return new_index;
}

/***************************外键约束处理**********************************/
static void dict_foreign_free(dict_foreign_t* foreign)
{
	mem_heap_free(foreign->heap);
}

static void dict_foreign_remove_from_cache(dict_foreign_t*	foreign)
{
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_a(foreign);

	if (foreign->referenced_table)
		UT_LIST_REMOVE(referenced_list, foreign->referenced_table->referenced_list, foreign);

	if (foreign->foreign_table)
		UT_LIST_REMOVE(foreign_list, foreign->foreign_table->foreign_list, foreign);

	dict_foreign_free(foreign);
}

static dict_foreign_t* dict_foreign_find( dict_table_t*	table, char* id)
{
	dict_foreign_t*	foreign;

	ut_ad(mutex_own(&(dict_sys->mutex)));

	foreign = UT_LIST_GET_FIRST(table->foreign_list);
	while (foreign) {
		if (ut_strcmp(id, foreign->id) == 0)
			return(foreign);

		foreign = UT_LIST_GET_NEXT(foreign_list, foreign);
	}

	foreign = UT_LIST_GET_FIRST(table->referenced_list);
	while (foreign) {
		if (ut_strcmp(id, foreign->id) == 0)
			return(foreign);

		foreign = UT_LIST_GET_NEXT(referenced_list, foreign);
	}

	return NULL;
}

static dict_index_t* dict_foreign_find_index(dict_table_t*	table, char** columns, ulint n_cols, dict_index_t* types_idx)
{
	dict_index_t*	index;
	char*		col_name;
	ulint		i;
	
	index = dict_table_get_first_index(table);
	while (index != NULL) {
		if (dict_index_get_n_fields(index) >= n_cols) {
			for (i = 0; i < n_cols; i++) {
				col_name = dict_index_get_nth_field(index, i)->col->name;
				if (ut_strlen(columns[i]) != ut_strlen(col_name) || 0 != ut_cmp_in_lower_case(columns[i],col_name,ut_strlen(col_name)))
				  	break;

				if (types_idx && !cmp_types_are_equal(dict_index_get_nth_type(index, i),dict_index_get_nth_type(types_idx, i)))
				  	break;		
			}

			if (i == n_cols)
				return(index);
		}

		index = dict_table_get_next_index(index);
	}

	return(NULL);
}

ulint dict_foreign_add_to_cache(dict_foreign_t*	foreign)
{
	dict_table_t*	for_table;
	dict_table_t*	ref_table;
	dict_foreign_t*	for_in_cache			= NULL;
	dict_index_t*	index;
	ibool		added_to_referenced_list	= FALSE;

	ut_ad(mutex_own(&(dict_sys->mutex)));

	for_table = dict_table_check_if_in_cache_low(foreign->foreign_table_name);
	ref_table = dict_table_check_if_in_cache_low(foreign->referenced_table_name);

	ut_a(for_table || ref_table);

	if (for_table)
		for_in_cache = dict_foreign_find(for_table, foreign->id);

	if (!for_in_cache && ref_table)
		for_in_cache = dict_foreign_find(ref_table, foreign->id);

	if (for_in_cache)
		mem_heap_free(foreign->heap);
	else
		for_in_cache = foreign;

	if (for_in_cache->referenced_table == NULL && ref_table) {
		index = dict_foreign_find_index(ref_table, for_in_cache->referenced_col_names, for_in_cache->n_fields, for_in_cache->foreign_index);

		if (index == NULL){
			if (for_in_cache == foreign)
				mem_heap_free(foreign->heap);

			return(DB_CANNOT_ADD_CONSTRAINT);
		}

		for_in_cache->referenced_table = ref_table;
		for_in_cache->referenced_index = index;
		UT_LIST_ADD_LAST(referenced_list, ref_table->referenced_list, for_in_cache);
		added_to_referenced_list = TRUE;
	}

	if (for_in_cache->foreign_table == NULL && for_table) {
		index = dict_foreign_find_index(for_table,for_in_cache->foreign_col_names,for_in_cache->n_fields,for_in_cache->referenced_index);

		if (index == NULL) {
			if (for_in_cache == foreign) {
				if (added_to_referenced_list)
					UT_LIST_REMOVE(referenced_list,ref_table->referenced_list,for_in_cache);

				mem_heap_free(foreign->heap);
			}

			return(DB_CANNOT_ADD_CONSTRAINT);
		}

		for_in_cache->foreign_table = for_table;
		for_in_cache->foreign_index = index;
		UT_LIST_ADD_LAST(foreign_list,for_table->foreign_list,for_in_cache);
	}

	return(DB_SUCCESS);
}

static char* dict_scan_to(char*	ptr, char*	string)
{
	ibool	success;
	ulint	i;
loop:
	if (*ptr == '\0')
		return(ptr);

	success = TRUE;

	for (i = 0; i < ut_strlen(string); i++) {
		if (toupper((ulint)(ptr[i])) != toupper((ulint)(string[i]))) {
			success = FALSE;
			break;
		}
	}

	if (success) return(ptr);

	ptr++;

	goto loop;
}

static char* dict_accept(char* ptr, char* string, ibool* success)
{
	char*	old_ptr = ptr;
	char*	old_ptr2;

	*success = FALSE;

	while (isspace(*ptr)) {
		ptr++;
	}

	old_ptr2 = ptr;
	ptr = dict_scan_to(ptr, string);

	if (*ptr == '\0' || old_ptr2 != ptr) 
		return(old_ptr)

	*success = TRUE;

	return(ptr + ut_strlen(string));
}

static char* dict_scan_col(char* ptr, ibool* success, dict_table_t* table, dict_col_t** column, char** column_name, ulint* column_name_len)
{
	dict_col_t*	col;
	char*		old_ptr;
	ulint		i;

	*success = FALSE;

	while (isspace(*ptr)) {
		ptr++;
	}

	if (*ptr == '\0') 
		return(ptr);

	if (*ptr == '`') 
		ptr++;

	old_ptr = ptr;

	while (!isspace(*ptr) && *ptr != ',' && *ptr != ')' && 	*ptr != '`') 
		ptr++;

	*column_name_len = (ulint)(ptr - old_ptr);

	if (table == NULL) {
		*success = TRUE;
		*column = NULL;
		*column_name = old_ptr;
	} 
	else {
		for (i = 0; i < dict_table_get_n_cols(table); i++) {
			col = dict_table_get_nth_col(table, i);
			if (ut_strlen(col->name) == (ulint)(ptr - old_ptr) && 0 == ut_cmp_in_lower_case(col->name, old_ptr, (ulint)(ptr - old_ptr))) {
				*success = TRUE;
				*column = col;
				*column_name = col->name;

				break;
			}
		}
	}

	if (*ptr == '`')
		ptr++;

	return(ptr);
}

static char* dict_scan_table_name(char*	 ptr, dict_table_t** table, char* name, ibool* success, char* second_table_name)
{
	char*	dot_ptr			= NULL;
	char*	old_ptr;
	ulint	i;
	
	*success = FALSE;
	*table = NULL;

	while (isspace(*ptr))
		ptr++;

	if (*ptr == '\0') 
		return(ptr);

	if (*ptr == '`')
		ptr++;

	old_ptr = ptr;
	while (!isspace(*ptr) && *ptr != '(' && *ptr != '`') {
		if (*ptr == '.')
			dot_ptr = ptr;

		ptr++;
	}

	if (ptr - old_ptr > 2000)
		return(old_ptr);
	
	if (dot_ptr == NULL) {
		/* Copy the database name from 'name' to the start */
		for (i = 0;; i++) {
			second_table_name[i] = name[i];
			if (name[i] == '/') {
				i++;
				break;
			}
		}
		ut_memcpy(second_table_name + i, old_ptr, ptr - old_ptr);
		second_table_name[i + (ptr - old_ptr)] = '\0';
	} 
	else {
		ut_memcpy(second_table_name, old_ptr, ptr - old_ptr);
		second_table_name[dot_ptr - old_ptr] = '/';
		second_table_name[ptr - old_ptr] = '\0';
	}

	*success = TRUE;

	*table = dict_table_get_low(second_table_name);
	if (*ptr == '`')
		ptr++;

	return(ptr);
}

static int dict_bracket_count(char* string, char* ptr)
{
	int	count	= 0;

	while (string != ptr) {
		if (*string == '(')  count++;
		if (*string == ')')count--;

		string++;
	}

	return(count);
}

ulint dict_create_foreign_constraints(trx_t* trx, char*	sql_string,	char* name)
{
	dict_table_t*	table;
	dict_table_t*	referenced_table;
	dict_index_t*	index;
	dict_foreign_t*	foreign;
 	char*		ptr				= sql_string;
	ibool		success;
	ulint		error;
	ulint		i;
	ulint		j;
	dict_col_t*	columns[500];
	char*		column_names[500];
	ulint		column_name_lens[500];
	char		referenced_table_name[2500];
	
	ut_ad(mutex_own(&(dict_sys->mutex)));

	table = dict_table_get_low(name);

	if (table == NULL)
		return(DB_ERROR);


loop:
	ptr = dict_scan_to(ptr, "FOREIGN");

	if (*ptr == '\0') {
		error = dict_create_add_foreigns_to_dictionary(table, trx);
		return(error);
	}

	ptr = dict_accept(ptr, "FOREIGN", &success);		
	if (!isspace(*ptr))
	        goto loop;

	ptr = dict_accept(ptr, "KEY", &success);
	if (!success) 
		goto loop;

	ptr = dict_accept(ptr, "(", &success);
	if (!success)
		goto loop;

	i = 0;
col_loop1:
	ptr = dict_scan_col(ptr, &success, table, columns + i, column_names + i, column_name_lens + i);
	if (!success)
		return(DB_CANNOT_ADD_CONSTRAINT);

	i++;
	
	ptr = dict_accept(ptr, ",", &success);
	if (success)
		goto col_loop1;
	
	ptr = dict_accept(ptr, ")", &success);
	if (!success)
		return(DB_CANNOT_ADD_CONSTRAINT);

	/* Try to find an index which contains the columns
	as the first fields and in the right order */
	index = dict_foreign_find_index(table, column_names, i, NULL);

	if (!index)
		return(DB_CANNOT_ADD_CONSTRAINT);

	ptr = dict_accept(ptr, "REFERENCES", &success);
	if (!success || !isspace(*ptr))
		return(DB_CANNOT_ADD_CONSTRAINT);

	/* Let us create a constraint struct */
	foreign = dict_mem_foreign_create();

	foreign->foreign_table = table;
	foreign->foreign_table_name = table->name;
	foreign->foreign_index = index;
	foreign->n_fields = i;
	foreign->foreign_col_names = mem_heap_alloc(foreign->heap, i * sizeof(void*));
	for (i = 0; i < foreign->n_fields; i++) {
		foreign->foreign_col_names[i] = mem_heap_alloc(foreign->heap, 1 + ut_strlen(columns[i]->name));
		ut_memcpy(foreign->foreign_col_names[i], columns[i]->name, 1 + ut_strlen(columns[i]->name));
	}
	
	ptr = dict_scan_table_name(ptr, &referenced_table, name, &success, referenced_table_name);

	/* Note that referenced_table can be NULL if the user has suppressed
	checking of foreign key constraints! */

	if (!success || (!referenced_table && trx->check_foreigns)) {
		dict_foreign_free(foreign);
		return(DB_CANNOT_ADD_CONSTRAINT);
	}
	
	ptr = dict_accept(ptr, "(", &success);
	if (!success) {
		dict_foreign_free(foreign);
		return(DB_CANNOT_ADD_CONSTRAINT);
	}

	/* Scan the columns in the second list */
	i = 0;
col_loop2:
	ptr = dict_scan_col(ptr, &success, referenced_table, columns + i, column_names + i, column_name_lens + i);
	i++;
	
	if (!success) {
		dict_foreign_free(foreign);
		return(DB_CANNOT_ADD_CONSTRAINT);
	}

	ptr = dict_accept(ptr, ",", &success);

	if (success)
		goto col_loop2;
	
	ptr = dict_accept(ptr, ")", &success);
	if (!success || foreign->n_fields != i) {
		dict_foreign_free(foreign);
		return(DB_CANNOT_ADD_CONSTRAINT);
	}

	ptr = dict_accept(ptr, "ON", &success);
	if (!success)
		goto try_find_index;

	ptr = dict_accept(ptr, "DELETE", &success);

	if (!success)
		goto try_find_index;

	ptr = dict_accept(ptr, "CASCADE", &success);

	if (success){
		foreign->type = DICT_FOREIGN_ON_DELETE_CASCADE;
		goto try_find_index;
	}

	ptr = dict_accept(ptr, "SET", &success);
	if (!success)
		goto try_find_index;

	ptr = dict_accept(ptr, "NULL", &success);
	if (success) {
		for (j = 0; j < foreign->n_fields; j++) {
			if ((dict_index_get_nth_type(foreign->foreign_index, j)->prtype) & DATA_NOT_NULL) {
				dict_foreign_free(foreign);
				return(DB_CANNOT_ADD_CONSTRAINT);
			}
		}

		foreign->type = DICT_FOREIGN_ON_DELETE_SET_NULL;
		goto try_find_index;
	}
	
try_find_index:
	/* Try to find an index which contains the columns as the first fields
	and in the right order, and the types are the same as in
	foreign->foreign_index */

	if (referenced_table) {
		index = dict_foreign_find_index(referenced_table,column_names, i,foreign->foreign_index);
		if (!index) {
			dict_foreign_free(foreign);
			return(DB_CANNOT_ADD_CONSTRAINT);
		}
	} 
	else {
		ut_a(trx->check_foreigns == FALSE);
		index = NULL;
	}

	foreign->referenced_index = index;
	foreign->referenced_table = referenced_table;

	foreign->referenced_table_name = mem_heap_alloc(foreign->heap, 1 + ut_strlen(referenced_table_name));

	ut_memcpy(foreign->referenced_table_name, referenced_table_name, 1 + ut_strlen(referenced_table_name));
					
	foreign->referenced_col_names = mem_heap_alloc(foreign->heap, i * sizeof(void*));
	for (i = 0; i < foreign->n_fields; i++) {
		foreign->referenced_col_names[i] = mem_heap_alloc(foreign->heap,1 + column_name_lens[i]);
		ut_memcpy(foreign->referenced_col_names[i], column_names[i],column_name_lens[i]);
		(foreign->referenced_col_names[i])[column_name_lens[i]] = '\0';
	}

	/* We found an ok constraint definition: add to the lists */
	
	UT_LIST_ADD_LAST(foreign_list, table->foreign_list, foreign);

	if (referenced_table)
		UT_LIST_ADD_LAST(referenced_list, referenced_table->referenced_list, foreign);

	goto loop;
}
/************************外键约束处理函数实现结束***************************************/




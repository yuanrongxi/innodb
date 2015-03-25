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

UNIV_INLINE void dict_index_add_col(dict_index_t* index, dict_col_t* col);

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


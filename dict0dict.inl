
#include "dict0load.h"
#include "trx0undo.h"
#include "trx0sys.h"
#include "rem0rec.h"

/*获得column的类型*/
UNIV_INLINE dtype_t* dict_col_get_type(dict_col_t* col)
{
	ut_ad(col);
	return &(col->type);
}

/*获得column的在表中的偏移*/
UNIV_INLINE ulint dict_col_get_no(dict_col_t* col)
{
	ut_ad(col);
	return col->ind;
}
/*获得该列在聚集索引中的偏移*/
UNIV_INLINE ulint dict_col_get_clust_pos(dict_col_t* col)
{
	ut_ad(col);
	return col->clust_pos;
}

/*获得表中第一个索引对象*/
UNIV_INLINE dict_index_t* dict_table_get_first_index(dict_table_t* table)
{
	ut_ad(table);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	return UT_LIST_GET_FIRST(table->indexes);
}

/*获得index的下一个索引对象*/
UNIV_INLINE dict_index_t* dict_table_get_next_index(dict_index_t* index)
{
	ut_ad(index);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

	return UT_LIST_GET_NEXT(indexes, index);
}

/*获得表中用户定义的列数*/
UNIV_INLINE ulint dict_table_get_n_user_cols(dict_table_t* table)
{
	ut_ad(table);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
	ut_ad(table->cached);

	return table->n_cols - DATA_N_SYS_COLS;
}

/*获得表的系统列数*/
UNIV_INLINE dict_table_get_n_sys_cols(dict_table_t* table)
{
	ut_ad(table);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
	ut_ad(table->cached);

	return DATA_N_SYS_COLS;
}
/*获得表的总列数，包括系统列*/
UNIV_INLINE ulint dict_table_get_n_cols(dict_table_t* table)
{
	ut_ad(table);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
	ut_ad(table->cached);

	return table->n_cols;
}

/*获得表的第N列对象*/
UNIV_INLINE dict_col_t* dict_table_get_nth_col(dict_table_t* table, ulint pos)
{
	ut_ad(table);
	ut_ad(pos < table->n_def);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	return table->cols + pos;
}

/*获得表中第N系统列对象*/
UNIV_INLINE dict_col_t* dict_table_get_sys_col(dict_table_t* table, ulint sys)
{
	dict_col_t*	col;

	ut_ad(table);
	ut_ad(sys < DATA_N_SYS_COLS);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	col = dict_table_get_nth_col(table, table->n_cols - DATA_N_SYS_COLS + sys);

	ut_ad(col->type.mtype == DATA_SYS);
	ut_ad(col->type.prtype == sys);

	return col;
}
/*获得sys系统列位置在所有列的排序位置*/
UNIV_INLINE ulint dict_table_get_sys_col_no(dict_table_t* table, ulint sys)
{
	ut_ad(table);
	ut_ad(sys < DATA_N_SYS_COLS);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

	return(table->n_cols - DATA_N_SYS_COLS + sys);
}

/*获得索引对象对应的列数量*/
UNIV_INLINE ulint dict_index_get_n_fields(dict_index_t* index)
{
	ut_ad(index);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
	ut_ad(index->cached);

	return index->n_fields;
}

/*获得索引对象的能确定唯一性的列数*/
UNIV_INLINE ulint dict_index_get_n_unique(dict_index_t* index)
{	
	ut_ad(index);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
	ut_ad(index->cached);

	return index->n_uniq;
}

/*确定索引唯一性的列数*/
UNIV_INLINE ulint dict_index_get_n_unique_in_tree(dict_index_t* index)
{
	ut_ad(index);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
	ut_ad(index->cached);

	if(index->type & DICT_CLUSTERED)
		return dict_index_get_n_unique(index);

	return dict_index_get_n_fields(index);
}

/************************************************************************
Gets the number of user-defined ordering fields in the index. In the internal
representation of clustered indexes we add the row id to the ordering fields
to make a clustered index unique, but this function returns the number of
fields the user defined in the index as ordering fields. */
UNIV_INLINE ulint dict_index_get_n_ordering_defined_by_user(dict_index_t* index)
{
	return index->n_user_defined_cols;
}

/*获得索引对象第N个位置上的列*/
UNIV_INLINE dict_field_t* dict_index_get_nth_field(dict_index_t* index, ulint pos)
{
	ut_ad(index);
	ut_ad(pos < index->n_def);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

	return index->fields + pos;
}

/*返回一个系统列在索引对象的位置*/
UNIV_INLINE ulint dict_index_get_sys_col_pos(dict_index_t* index, ulint type)
{
	dict_col_t*	col;

	ut_ad(index);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
	ut_ad(!(index->type & DICT_UNIVERSAL));

	col = dict_table_get_sys_col(index->table, type);
	if(index->type & DICT_CLUSTERED)
		return col->clust_pos;

	return dict_index_get_nth_col_pos(index, dict_table_get_sys_col_no(index->table, type));
}
/*获得记录rec中系统列的值，row_id/roll_ptr/trx_id/mix_id*/
UNIV_INLINE dulint dict_index_rec_get_sys_col(dict_index_t* index, ulint type, rec_t* rec)
{
	ulint	pos;
	byte*	field;
	ulint	len;

	ut_ad(index);
	ut_ad(index->type & DICT_CLUSTERED);

	pos = dict_index_get_sys_col_pos(index, type); 	/*确定系统列位置*/
	field = rec_get_nth_field(rec, pos, &len);

	if (type == DATA_ROLL_PTR) {
		ut_ad(len == 7);
		return(trx_read_roll_ptr(field));
	} 
	else if ((type == DATA_ROW_ID) || (type == DATA_MIX_ID)){
		return(mach_dulint_read_compressed(field));
	}
	else{
		ut_ad(type == DATA_TRX_ID);
		return(trx_read_trx_id(field));
	}
}

/*获得索引对象对应的索引树对象*/
UNIV_INLINE dict_tree_t* dict_index_get_tree(dict_index_t* index)
{
	ut_ad(index);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

	return index->tree;
}
/*获得列的排序标识*/
UNIV_INLINE ulint dict_field_get_order(dict_field_t* field)
{
	ut_ad(field);
	return field->order;
}

/*获得field对应的column对象*/
UNIV_INLINE dict_col_t* dict_field_get_col(dict_field_t* field)
{
	ut_ad(field);
	return field->col;
}

/*获得索引对象中第N个依赖列中的数据类型*/
UNIV_INLINE dtype_t* dict_index_get_nth_type(dict_index_t* index, ulint pos)
{
	return dict_col_get_type(dict_field_get_col(dict_index_get_nth_field(index, pos)));
}

/*获得索引树root page对应的表空间ID*/
UNIV_INLINE ulint dict_tree_get_space(dict_tree_t* tree)
{
	ut_ad(tree);
	ut_ad(tree->magic_n == DICT_TREE_MAGIC_N);

	return tree->space;
}
/*设置索引树对象的root page对应的表空间ID*/
UNIV_INLINE void dict_tree_set_space(dict_tree_t* tree, ulint space)
{
	ut_ad(tree);
	ut_ad(tree->magic_n == DICT_TREE_MAGIC_N);

	tree->space = space;
}

/*获得索引树的root page对应的page no*/
UNIV_INLINE ulint dict_tree_get_page(dict_tree_t* tree)
{
	ut_ad(tree);
	ut_ad(tree->magic_n == DICT_TREE_MAGIC_N);

	return tree->page;
}

/*获得索引树的type*/
UNIV_INLINE ulint dict_tree_get_type(dict_tree_t* tree)
{
	ut_ad(tree);
	ut_ad(tree->magic_n == DICT_TREE_MAGIC_N);

	return tree->type;
}

/*获得索引树的rw_lock*/
UNIV_INLINE rw_lock_t* dict_tree_get_lock(dict_tree_t* tree)
{
	ut_ad(tree);
	ut_ad(tree->magic_n == DICT_TREE_MAGIC_N);

	return &(tree->lock);
}

/*获得tree对应的表空间预留给更新记录用的空间大小, 1/16 page size*/
UNIV_INLINE ulint dict_tree_get_space_reserve(dict_tree_t* tree)
{
	ut_ad(tree);

	UT_NOT_USED(tree);

	return (UNIV_PAGE_SIZE / 16);
}

/*判断表是否在数据字典的cache当中，如果在，返回对应的数据字典对象*/
UNIV_INLINE dict_table_t* dict_table_check_if_in_cache_low(char* name)
{
	dict_table_t*	table;
	ulint			table_fold;

	ut_ad(table_name);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	table_fold = ut_fold_string(name);

	/*在dict_sys->table_hash中查找*/
	HASH_SEARCH(name_hash, dict_sys->table_hash, table_fold, table, ut_strcmp(table->name, table_name) == 0);

	return table;
}

/*通过表名获得表的数据字典对象*/
UNIV_INLINE dict_table_t* dict_table_get_low(char* table_name)
{
	dict_table_t* table;

	ut_ad(table_name);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	table = dict_table_check_if_in_cache_low(table_name);
	if(table == NULL)
		table = dict_load_table(table_name); /*从磁盘上导入table_name的表数据字典到内存中*/

	return table;
}

UNIV_INLINE dict_proc_t* dict_procedure_get(char* proc_name, trx_t* trx)
{
	dict_proc_t*	proc;
	ulint		name_fold;

	UT_NOT_USED(trx);

	mutex_enter(&(dict_sys->mutex));

	name_fold = ut_fold_string(proc_name);

	HASH_SEARCH(name_hash, dict_sys->procedure_hash, name_fold, proc, ut_strcmp(proc->name, proc_name) == 0);
	if (proc != NULL) 
		proc->mem_fix++;

	mutex_exit(&(dict_sys->mutex));

	return(proc);
}

/*通过table id查找对应表的数据字典对象*/
UNIV_INLINE dict_table_t* dict_table_get_on_id_low(dulint table_id, trx_t* trx)
{
	dict_table_t* table;
	ulint fold;

	ut_ad(mutex_own(&(dict_sys->mutex)));
	UT_NOT_USED(trx);

	fold = ut_fold_dulint(table_id);
	HASH_SEARCH(id_hash, dict_sys->table_id_hash, fold, table, ut_dulint_cmp(table->id, table_id) == 0);
	if(table == NULL)
		table = dict_load_table_on_id(table_id); /*从磁盘上导入对应表的数据字典*/

	if(table != NULL)
		table->mem_fix ++;

	return table;
}

UNIV_INLINE void dict_table_release(dict_table_t* table)
{
	mutex_enter(&(dict_sys->mutex));
	table->mem_fix --;
	mutex_exit(&(dict_sys->mutex));
}

/*通过索引名获得表对应的索引对象*/
UNIV_INLINE dict_index_t* dict_table_get_index(dict_table_t* table, char* name)
{
	dict_index_t* index = NULL;
	index = dict_table_get_first_index(table);
	while(index != NULL){
		if(ut_strcmp(name, index->name) == 0)
			break;

		index = dict_table_get_next_index(index);
	}

	return index;
}

/*检查表的mix_id_buf是否是rec的内容*/
UNIV_INLINE ibool dict_is_mixed_table_rec(dict_table_t* table, rec_t* rec)
{
	byte* mix_id_field;
	ulint len;

	/*获得rec的mix_len对应的列*/
	mix_id_field = rec_get_nth_field(rec, table->mix_len, &len);
	if(len != table->mix_id_len || (0 != ut_memcmp(table->mix_id_buf, mix_id_field, len)))
		return FALSE;

	return TRUE;
}


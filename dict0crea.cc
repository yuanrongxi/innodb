#include "dict0crea.h"

#include "btr0pcur.h"
#include "btr0btr.h"
#include "page0page.h"
#include "mach0data.h"
#include "dict0boot.h"
#include "dict0dict.h"
#include "que0que.h"
#include "row0ins.h"
#include "row0mysql.h"
#include "pars0pars.h"
#include "trx0roll.h"
#include "usr0sess.h"


static dtuple_t* dict_create_sys_tables_tuple(dict_table_t* table, mem_heap_t* heap);
static dtuple_t* dict_create_sys_columns_tuple(dict_table_t* table, ulint i, mem_heap_t* heap);
static dtuple_t* dict_create_sys_indexes_tuple(dict_index_t* index, mem_heap_t* heap, trx_t* trx);
static dtuple_t* dict_create_sys_fields_tuple(dict_index_t* index, ulint i, mem_heap_t* heap);
static dtuple_t* dict_create_search_tuple(dict_table_t* table, mem_heap_t* heap);
/*************************************************************************************/

/*将table的字典信息构建一条sys table的内存逻辑记录对象*/
static dtuple_t* dict_create_sys_tables_tuple(dict_table_t* table, mem_heap_t* heap)
{
	dict_table_t*	sys_tables;
	dtuple_t*		entry;
	dfield_t*		dfield;
	byte*			ptr;

	ut_ad(table && heap);

	sys_tables = dict_sys->sys_tables;

	/*构建一条sys table表中的逻辑记录对象*/
	entry = dtuple_create(heap, 8 + DATA_N_SYS_COLS);
	/*NAME*/
	dfield = dtuple_get_nth_field(entry, 0);
	dfield_set_data(dfield, table->name, ut_strlen(table->name));
	/*ID*/
	dfield = dtuple_get_nth_field(entry, 1);
	ptr = mem_heap_alloc(heap, 8);
	mach_write_to_8(ptr, table->id);
	dfield_set_data(dfield, ptr, 8);
	/*N_COLS*/
	dfield = dtuple_get_nth_field(entry, 2);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, table->n_def);
	dfield_set_data(dfield, ptr, 4);
	/*TYPE*/
	dfield = dtuple_get_nth_field(entry, 3);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, table->type);
	dfield_set_data(dfield, ptr, 4);
	/*MIX ID*/
	dfield = dtuple_get_nth_field(entry, 4);
	ptr = mem_heap_alloc(heap, 8);
	mach_write_to_8(ptr, table->mix_id);
	dfield_set_data(dfield, ptr, 8);
	/*MIX_LEN*/
	dfield = dtuple_get_nth_field(entry, 5);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, table->mix_len);
	dfield_set_data(dfield, ptr, 4);
	/*CLUSTER NAME*/
	dfield = dtuple_get_nth_field(entry, 6);

	if (table->type == DICT_TABLE_CLUSTER_MEMBER) {
		dfield_set_data(dfield, table->cluster_name, ut_strlen(table->cluster_name));
		ut_a(0);
	} 
	else
		dfield_set_data(dfield, NULL, UNIV_SQL_NULL);

	/*SPACE ID*/
	dfield = dtuple_get_nth_field(entry, 7);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, table->space);
	dfield_set_data(dfield, ptr, 4);
	/*设置各个列的数据类型*/
	dict_table_copy_types(entry, sys_tables);

	return entry;
}

/*根据table的字典信息构建一条table第i列的SYS_COLUMNS表的内存逻辑记录对象*/
static dtuple_t* dict_create_sys_columns_tuple(dict_table_t* table, ulint i, mem_heap_t* heap)
{
	dict_table_t*	sys_columns;
	dtuple_t*	entry;
	dict_col_t*	column;
	dfield_t*	dfield;
	byte*		ptr;

	ut_ad(table && heap);

	column = dict_table_get_nth_col(table, i);
	sys_columns = dict_sys->sys_columns;

	entry = dtuple_create(heap, 7 + DATA_N_SYS_COLS);

	/* 0: TABLE_ID -----------------------*/
	dfield = dtuple_get_nth_field(entry, 0);
	ptr = mem_heap_alloc(heap, 8);
	mach_write_to_8(ptr, table->id);
	dfield_set_data(dfield, ptr, 8);
	/* 1: POS ----------------------------*/
	dfield = dtuple_get_nth_field(entry, 1);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, i);
	dfield_set_data(dfield, ptr, 4);
	/* 4: NAME ---------------------------*/
	dfield = dtuple_get_nth_field(entry, 2);
	dfield_set_data(dfield, column->name, ut_strlen(column->name));
	/* 5: MTYPE --------------------------*/
	dfield = dtuple_get_nth_field(entry, 3);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, (column->type).mtype);
	dfield_set_data(dfield, ptr, 4);
	/* 6: PRTYPE -------------------------*/
	dfield = dtuple_get_nth_field(entry, 4);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, (column->type).prtype);
	dfield_set_data(dfield, ptr, 4);
	/* 7: LEN ----------------------------*/
	dfield = dtuple_get_nth_field(entry, 5);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, (column->type).len);

	dfield_set_data(dfield, ptr, 4);
	/* 8: PREC ---------------------------*/
	dfield = dtuple_get_nth_field(entry, 6);

	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, (column->type).prec);

	dfield_set_data(dfield, ptr, 4);
	/*---------------------------------*/

	dict_table_copy_types(entry, sys_columns);
	return entry;
}

/*按照MYSQL定义的表对象，将元数据添加到SYS TABLE中*/
static ulint dict_build_table_def_step(que_thr_t* thr, tab_node_t* node)
{
	dict_table_t*	table;
	dict_table_t*	cluster_table;
	dtuple_t*	row;

	UT_NOT_USED(thr);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	table = node->table;
	table->id = dict_hdr_get_new_id(DICT_HDR_TABLE_ID);
	thr_get_trx(thr)->table_id = table->id;

	if(table->type == DICT_TABLE_CLUSTER_MEMBER){
		cluster_table = dict_table_get_low(table->cluster_name);
		if(cluster_table == NULL)
			return DB_CLUSTER_NOT_FOUND;
		/*设置表空间，依赖于cluster table*/
		table->space = cluster_table->space;
		table->mix_len = cluster_table->mix_len;
		/*获取一个不重复的mix id*/
		table->mix_id = dict_hdr_get_new_id(DICT_HDR_MIX_ID);
	}

	row = dict_create_sys_tables_tuple(table, node->heap);
	ins_node_set_new_row(node->table_def, row);

	return DB_SUCCESS;
}

/*向node->table的第i的字典信息加入到SYS COLUMN中*/
static ulint dict_build_col_def_step(tab_node_t* node)
{
	dtuple_t*	row;

	row = dict_create_sys_columns_tuple(node->table, node->col_no, node->heap);
	ins_node_set_new_row(node->col_ref, row);
	
	return DB_SUCCESS;
}

/*将索引对象index中的字典信息按照SYS INDEX表的记录格式创建一条内存记录对象(tuple)*/
static dtuple_t* dict_create_sys_indexes_tuple(dict_index_t* index, mem_heap_t* heap, trx_t* trx)
{
	dict_table_t*	sys_indexes;
	dict_table_t*	table;
	dtuple_t*	entry;
	dfield_t*	dfield;
	byte*		ptr;

	UT_NOT_USED(trx);
	ut_ad(mutex_own(&(dict_sys->mutex)));
	ut_ad(index && heap);

	sys_indexes = dict_sys->sys_indexes;
	/*产生tuple记录对象*/
	table = dict_table_get_low(index->table_name);
	entry = dtuple_create(heap, 7 + DATA_N_SYS_COLS);

	/* 0: TABLE_ID -----------------------*/
	dfield = dtuple_get_nth_field(entry, 0);
	ptr = mem_heap_alloc(heap, 8);
	mach_write_to_8(ptr, table->id);
	dfield_set_data(dfield, ptr, 8);
	/* 1: ID ----------------------------*/
	dfield = dtuple_get_nth_field(entry, 1);
	ptr = mem_heap_alloc(heap, 8);
	mach_write_to_8(ptr, index->id);
	dfield_set_data(dfield, ptr, 8);
	/* 4: NAME --------------------------*/
	dfield = dtuple_get_nth_field(entry, 2);
	dfield_set_data(dfield, index->name, ut_strlen(index->name));
	/* 5: N_FIELDS ----------------------*/
	dfield = dtuple_get_nth_field(entry, 3);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, index->n_fields);
	dfield_set_data(dfield, ptr, 4);

	/* 6: TYPE --------------------------*/
	dfield = dtuple_get_nth_field(entry, 4);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, index->type);
	dfield_set_data(dfield, ptr, 4);

	/* 7: SPACE --------------------------*/
	ut_a(DICT_SYS_INDEXES_SPACE_NO_FIELD == 7);
	dfield = dtuple_get_nth_field(entry, 5);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, index->space);
	dfield_set_data(dfield, ptr, 4);

	/* 8: PAGE_NO --------------------------*/
	ut_a(DICT_SYS_INDEXES_PAGE_NO_FIELD == 8);
	dfield = dtuple_get_nth_field(entry, 6);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, FIL_NULL);
	dfield_set_data(dfield, ptr, 4);
	/*--------------------------------*/

	dict_table_copy_types(entry, sys_indexes);

	return entry;
}

/*将index中第i个索引依赖的feild字典信息构建一条SYS FIELD的内存逻辑记录对象*/
static dtuple_t* dict_create_sys_fields_tuple(dict_index_t* index, ulint i, mem_heap_t* heap)
{
	dict_table_t*	sys_fields;
	dtuple_t*		entry;
	dict_field_t*	field;
	dfield_t*		dfield;
	byte*			ptr;

	ut_ad(index && heap);

	field = dict_index_get_nth_field(index, i);
	sys_fields = dict_sys->sys_fields;
	entry = dtuple_create(heap, 3 + DATA_N_SYS_COLS);

	/* 0: INDEX_ID -----------------------*/
	dfield = dtuple_get_nth_field(entry, 0);
	ptr = mem_heap_alloc(heap, 8);
	mach_write_to_8(ptr, index->id);
	dfield_set_data(dfield, ptr, 8);
	/* 1: POS ----------------------------*/
	dfield = dtuple_get_nth_field(entry, 1);
	ptr = mem_heap_alloc(heap, 4);
	mach_write_to_4(ptr, i);
	dfield_set_data(dfield, ptr, 4);
	/* 4: COL_NAME -------------------------*/
	dfield = dtuple_get_nth_field(entry, 2);
	dfield_set_data(dfield, field->name, ut_strlen(field->name));
	/*---------------------------------*/

	dict_table_copy_types(entry, sys_fields);

	return entry;
}

/*获得一个可以进行索引搜索定位的dtuple*/
static dtuple_t* dict_create_search_tuple(dtuple_t* tuple, mem_heap_t* heap)
{
	dtuple_t*	search_tuple;
	dfield_t*	field1;
	dfield_t*	field2;

	ut_ad(tuple && heap);

	search_tuple = dtuple_create(heap, 2);

	field1 = dtuple_get_nth_field(tuple, 0);	
	field2 = dtuple_get_nth_field(search_tuple, 0);	
	dfield_copy(field2, field1);

	field1 = dtuple_get_nth_field(tuple, 1);	
	field2 = dtuple_get_nth_field(search_tuple, 1);	
	dfield_copy(field2, field1);

	ut_ad(dtuple_validate(search_tuple));

	return search_tuple;
}

/*通过node中的信息构建一个SYS INDEX表中的索引对象记录,并插入SYS INDEX中*/
static ulint dict_build_index_def_step(que_thr_t* thr, ind_node_t* node)
{
	dict_table_t*	table;
	dict_index_t*	index;
	dtuple_t*	row;

	UT_NOT_USED(thr);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	index = node->index;
	table = dict_table_get_low(index->table_name);
	if(table == NULL)
		return DB_TABLE_NOT_FOUND;
	/*设置table id*/
	thr_get_trx(thr)->table->id = table->id;

	node->table = table;
	ut_ad((UT_LIST_GET_LEN(table->indexes) > 0) || (index->type & DICT_CLUSTERED));
	/*为新索引分配一个索引的ID*/
	index->id = dict_hdr_get_new_id(DICT_HDR_INDEX_ID);
	if(index->type & DICT_CLUSTERED)
		index->space = table->space;

	index->page_no = FIL_NULL;
	/*构建一个index记录tuple*/
	row = dict_create_sys_indexes_tuple(index, node->heap, thr_get_trx(thr));
	node->ind_row = row;

	ins_node_set_new_row(node->ind_def, row);

	return DB_SUCCESS;
}

/*构建一个field格式为SYS_FIELD表的内存行记录tuple对象，并将row插入到SYS_FIELD中*/
static ulint dict_build_field_def_step(ind_node_t* node)
{
	dict_index_t*	index;
	dtuple_t*	row;

	index = node->index;
	row = dict_create_sys_fields_tuple(index, node->field_no, node->heap);
	ins_node_set_new_row(node->field_def, row);

	return DB_SUCCESS;
}

/*构建不是cluster member的索引对象的索引树*/
static ulint dict_create_index_tree_step(que_thr_t* thr, ind_node_t* node)
{
	dict_index_t*	index;
	dict_table_t*	sys_indexes;
	dict_table_t*	table;
	dtuple_t*	search_tuple;
	btr_pcur_t	pcur;
	mtr_t		mtr;

	ut_ad(mutex_own(&(dict_sys->mutex)));
	UT_NOT_USED(thr);

	index = node->index;	
	table = node->table;

	sys_indexes = dict_sys->sys_indexes;
	if(index->type & DICT_CLUSTERED && table->type == DICT_TABLE_CLUSTER_MEMBER)
		return DB_SUCCESS;
	
	/*启动一个mini transaction*/
	mtr_start(&mtr);

	search_tuple = dict_create_search_tuple(node->ind_row, node->heap);
	/*打开SYS INDEX表聚集索引树并定位到search_tuple的BTREE位置*/
	btr_pcur_open(UT_LIST_GET_FIRST(sys_indexes->indexes), search_tuple, PAGE_CUR_L, BTR_MODIFY_LEAF, &pcur, &mtr);
	btr_pcur_move_to_next_user_rec(&pcur, &mtr);

	/*在index->space的表空间上为新建的索引添加个btree*/
	index->page_no = btr_create(index->type, index->space, index->id, &mtr);
	/*将新创建的page no写入到sys index表中*/
	page_rec_write_index_page_no(btr_pcur_get_rec(&pcur), DICT_SYS_INDEXES_PAGE_NO_FIELD, index->page_no, &mtr);
	btr_pcur_close(&pcur);

	if(index->page_no == FIL_NULL)
		return DB_OUT_OF_FILE_SPACE;

	return DB_SUCCESS;
}

/*删除一个索引树对象*/
void dict_drop_index_tree(rec_t* rec, mtr_t* mtr)
{
	ulint	root_page_no;
	ulint	space;
	byte*	ptr;
	ulint	len;

	ut_ad(mutex_own(&(dict_sys->mutex)));
	/*获得SYS INDEX表中对应的root page no*/
	ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, &len);
	ut_ad(len == 4);
	root_page_no = mtr_read_ulint(ptr, MLOG_4BYTES, mtr);
	if(root_page_no == FIL_NULL)
		return ;

	/*获得space id*/
	ptr = rec_get_nth_field(rec, DICT_SYS_INDEXES_SPACE_NO_FIELD, &len);
	space = mtr_read_ulint(ptr, MLOG_4BYTES, mtr);
	/*释放整个btree的page,root page不做释放*/
	btr_free_but_not_root(space, root_page_no);
	/*对root page的释放*/
	btr_free_root(space, root_page_no, mtr);

	/*更新SYS INDEX对应记录的root page no*/
	page_rec_write_index_page_no(rec, DICT_SYS_INDEXES_PAGE_NO_FIELD, FIL_NULL, mtr);
}

/*创建一个建表的create graph*/
tab_node_t* tab_create_graph_create(dict_table_t* table, mem_heap_t* heap)
{
	tab_node_t* node;

	node = mem_heap_alloc(heap, sizeof(tab_node_t));
	node->common.type = QUE_NODE_CREATE_TABLE;

	node->table = table;
	node->state = TABLE_BUILD_TABLE_DEF;
	node->heap = mem_heap_create(256);

	node->table_ref = ins_node_create(INS_DIRECT, dict_sys->sys_tables, heap);
	node->table_ref->common.parent = node;
	
	node->col_ref = ins_node_create(INS_DIRECT, dict_sys->sys_columns, heap);
	node->col_ref->common.parent = node;

	node->commit_node = commit_node_create(heap);
	node->commit_node->common.parent = node;

	return node;
}

/*创建一个建索引的create graph*/
ind_node_t* ind_create_graph_create(dict_index_t* index, mem_heap_t* heap)
{
	ind_node_t* node;
	
	node = mem_heap_alloc(heap, sizeof(ind_node_t));
	node->common.type = QUE_NODE_CREATE_INDEX;
	node->index = index;

	node->state = INDEX_BUILD_INDEX_DEF;
	node->heap = mem_heap_create(256);

	node->ind_def = ins_node_create(INS_DIRECT, dict_sys->sys_indexes, heap);
	node->ind_def->common.parent = node;

	node->field_def = ins_node_create(INS_DIRECT, dict_sys->sys_fields, heap);
	node->field_def->common.parent = node;

	return node;
}

/*执行一个建表的que thread*/
que_thr_t* dict_create_table_step(que_thr_t* thr)
{
	tab_node_t*	node;
	ulint		err	= DB_ERROR;
	trx_t*		trx;

	ut_ad(thr);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	trx = thr_get_trx(thr);
	node = thr->run_node;

	ut_ad(que_node_get_type(node) == QUE_NODE_CREATE_TABLE);
	if(thr->prev_node == que_node_get_parent(node))
		node->state = TABLE_BUILD_TABLE_DEF;

	if(node->state == TABLE_BUILD_TABLE_DEF){
		/*先将node中的建表信息写入到sys table中*/
		err = dict_build_table_def_step(thr, node);
		if(err != DB_SUCCESS)
			goto function_eixt;

		node->state = TABLE_BUILD_COL_DEF;
		node->col_no = 0;

		thr->run_node = node->table_ref;
		
		return thr;
	}

	/*建列任务*/
	if(node->state == TABLE_BUILD_COL_DEF){
		if(node->col_no < node->table->n_def){
			/*将各个列的信息写入到sys column表中*/
			err = dict_build_col_def_step(node);
			if(err != DB_SUCCESS)
				goto function_exit;

			node->col_no ++;
			thr->run_node = node->col_ref;
			return thr;
		}
		else /*可以进行commit work,将表导入数据字典cache中*/
			node->state = TABLE_COMMIT_WORK;
	}

	if(node->state == TABLE_COMMIT_WORK)
		node->state = TABLE_ADD_TO_CACHE;

	/*将表的数据字典导入cache中*/
	if(node->state == TABLE_ADD_TO_CACHE){
		dict_table_add_to_cache(node->table);
		err = DB_SUCCESS;
	}

function_exit:
	trx->error_state = err;
	if(err == DB_SUCCESS){

	}
	else if(err == DB_LOCK_WAIT)
		return NULL;
	else
		return NULL;

	/*孩子的que thread执行完毕，执行上一级的que thread*/
	thr->run_node = que_node_get_parent(node);

	return thr;
}

/*执行一个建索引的que thread*/
que_thr_t* dict_create_index_step(que_thr_t* thr)
{
	ind_node_t*	node;
	ibool		success;
	ulint		err	= DB_ERROR;
	trx_t*		trx;

	ut_ad(thr);
	ut_ad(mutex_own(&(dict_sys->mutex)));

	trx = thr_get_trx(thr);
	node = thr->run_node;

	ut_ad(que_node_get_type(node) == QUE_NODE_CREATE_INDEX);
	if(thr->prev_node == que_node_get_parent(node))
		node->state = INDEX_BUILD_INDEX_DEF;

	if(node->state == INDEX_BUILD_INDEX_DEF){ /*将index信息加入到sys_index表中*/
		err = dict_build_index_def_step(thr, node);
		if(err != DB_SUCCESS)
			goto function_exit;

		node->state = INDEX_BUILD_FIELD_DEF;
		node->field_no = 0;
		/*执行*/
		thr->run_node = node->ind_def;

		return(thr);
	}

	if (node->state == INDEX_BUILD_FIELD_DEF) { /*将index依赖的field信息加入到sys_field中*/
		if (node->field_no < (node->index)->n_fields) {

			err = dict_build_field_def_step(node);

			if (err != DB_SUCCESS)
				goto function_exit;

			node->field_no++;
			thr->run_node = node->field_def;

			return(thr);
		} 
		else  /*索引信息全部插入到INNODB的系统表中，可以建立对应的索引树对象*/
			node->state = INDEX_CREATE_INDEX_TREE;
	}

	/*建立索引树的que thread*/
	if (node->state == INDEX_CREATE_INDEX_TREE){
		err = dict_create_index_tree_step(thr, node);
		if (err != DB_SUCCESS)
			goto function_exit;

		node->state = INDEX_COMMIT_WORK;
	}

	if(node->state == INDEX_COMMIT_WORK)
		node->state = INDEX_ADD_TO_CACHE;

	if(node->state == INDEX_ADD_TO_CACHE){
		success = dict_index_add_to_cache(node->table, node->index);
		ut_a(success);
		err = DB_SUCCESS;
	}

function_exit:
	trx->error_state = err;
	if (err == DB_SUCCESS) {
	} 
	else if (err == DB_LOCK_WAIT)
		return(NULL);
	else
		return(NULL);

	thr->run_node = que_node_get_parent(node);

	return thr;
}

/********************************************************************
Creates the foreign key constraints system tables inside InnoDB
at database creation or database start if they are not found or are
not of the right form. */

ulint dict_create_or_check_foreign_constraint_tables(void)
{
	dict_table_t*	table1;
	dict_table_t*	table2;
	que_thr_t*	thr;
	que_t*		graph;
	ulint		error;
	trx_t*		trx;
	char*		str;

	mutex_enter(&(dict_sys->mutex));

	table1 = dict_table_get_low("SYS_FOREIGN");
	table2 = dict_table_get_low("SYS_FOREIGN_COLS");
	
	if (table1 && table2 && UT_LIST_GET_LEN(table1->indexes) == 3 && UT_LIST_GET_LEN(table2->indexes) == 1) {

		/* Foreign constraint system tables have already been
		created, and they are ok */

		mutex_exit(&(dict_sys->mutex));

		return(DB_SUCCESS);
	}

	trx = trx_allocate_for_mysql();
	trx->op_info = "creating foreign key sys tables";
	if (table1) {
		fprintf(stderr, "InnoDB: dropping incompletely created SYS_FOREIGN table\n");
		row_drop_table_for_mysql("SYS_FOREIGN", trx, TRUE);
	}

	if (table2) {
		fprintf(stderr,"InnoDB: dropping incompletely created SYS_FOREIGN_COLS table\n");
		row_drop_table_for_mysql("SYS_FOREIGN_COLS", trx, TRUE);
	}

	fprintf(stderr, "InnoDB: Creating foreign key constraint system tables\n");

	/* NOTE: in dict_load_foreigns we use the fact that
	there are 2 secondary indexes on SYS_FOREIGN, and they
	are defined just like below */
	
	str =
		"PROCEDURE CREATE_FOREIGN_SYS_TABLES_PROC () IS\n"
		"BEGIN\n"
		"CREATE TABLE\n"
		"SYS_FOREIGN(ID CHAR, FOR_NAME CHAR, REF_NAME CHAR, N_COLS INT);\n"
		"CREATE UNIQUE CLUSTERED INDEX ID_IND ON SYS_FOREIGN (ID);\n"
		"CREATE INDEX FOR_IND ON SYS_FOREIGN (FOR_NAME);\n"
		"CREATE INDEX REF_IND ON SYS_FOREIGN (REF_NAME);\n"
		"CREATE TABLE\n"
		"SYS_FOREIGN_COLS(ID CHAR, POS INT, FOR_COL_NAME CHAR, REF_COL_NAME CHAR);\n"
		"CREATE UNIQUE CLUSTERED INDEX ID_IND ON SYS_FOREIGN_COLS (ID, POS);\n"
		"COMMIT WORK;\n"
		"END;\n";

	/*向que thread管理器中加入一个创建外键约束的que thread*/
	graph = pars_sql(str);

	ut_a(graph);

	graph->trx = trx;
	trx->graph = NULL;

	graph->fork_type = QUE_FORK_MYSQL_INTERFACE;
	ut_a(thr = que_fork_start_command(graph, SESS_COMM_EXECUTE, 0));
	que_run_threads(thr);

	error = trx->error_state;
	if (error != DB_SUCCESS) {
		fprintf(stderr, "InnoDB: error %lu in creation\n", error);
		
		ut_a(error == DB_OUT_OF_FILE_SPACE);

		fprintf(stderr, "InnoDB: creation failed\n");
		fprintf(stderr, "InnoDB: tablespace is full\n");
		fprintf(stderr, "InnoDB: dropping incompletely created SYS_FOREIGN tables\n");

		row_drop_table_for_mysql("SYS_FOREIGN", trx, TRUE);
		row_drop_table_for_mysql("SYS_FOREIGN_COLS", trx, TRUE);

		error = DB_MUST_GET_MORE_FILE_SPACE;
	}

	que_graph_free(graph);
	trx->op_info = "";
  	trx_free_for_mysql(trx);

  	if (error == DB_SUCCESS) 
		fprintf(stderr, "InnoDB: Foreign key constraint system tables created\n");

	mutex_exit(&(dict_sys->mutex));

	return(error);
}

/************************************************************************
Adds foreign key definitions to data dictionary tables in the database. */

ulint
dict_create_add_foreigns_to_dictionary(
/*===================================*/
				/* out: error code or DB_SUCCESS */
	dict_table_t*	table,	/* in: table */
	trx_t*		trx)	/* in: transaction */
{
	dict_foreign_t*	foreign;
	que_thr_t*	thr;
	que_t*		graph;
	dulint		id;	
	ulint		len;
	ulint		error;
	ulint		i;
	char		buf2[50];
	char		buf[10000];

	ut_ad(mutex_own(&(dict_sys->mutex)));	

	if (NULL == dict_table_get_low("SYS_FOREIGN")) {
		fprintf(stderr, "InnoDB: table SYS_FOREIGN not found from internal data dictionary\n");
		return(DB_ERROR);
	}

	foreign = UT_LIST_GET_FIRST(table->foreign_list);
loop:
	if (foreign == NULL)
		return(DB_SUCCESS);

	/* Build an InnoDB stored procedure which will insert the necessary
	rows to SYS_FOREIGN and SYS_FOREIGN_COLS */

	len = 0;
	len += sprintf(buf, "PROCEDURE ADD_FOREIGN_DEFS_PROC () IS\nBEGIN\n");

	/* We allocate the new id from the sequence of table id's */
	id = dict_hdr_get_new_id(DICT_HDR_TABLE_ID);

	sprintf(buf2, "%lu_%lu", ut_dulint_get_high(id), ut_dulint_get_low(id));
	foreign->id = mem_heap_alloc(foreign->heap, ut_strlen(buf2) + 1);
	ut_memcpy(foreign->id, buf2, ut_strlen(buf2) + 1);
	
	len += sprintf(buf + len, "INSERT INTO SYS_FOREIGN VALUES('%lu_%lu', '%s', '%s', %lu);\n",
					ut_dulint_get_high(id),
					ut_dulint_get_low(id),
					table->name,
					foreign->referenced_table_name,
					foreign->n_fields
					+ (foreign->type << 24));

	for (i = 0; i < foreign->n_fields; i++) {
		len += sprintf(buf + len, "INSERT INTO SYS_FOREIGN_COLS VALUES('%lu_%lu', %lu, '%s', '%s');\n",
					ut_dulint_get_high(id),
					ut_dulint_get_low(id),
					i,
					foreign->foreign_col_names[i],
					foreign->referenced_col_names[i]);
	}

	len += sprintf(buf + len,"COMMIT WORK;\nEND;\n");

	/*解析SQL语句,得到一个que graph*/
	graph = pars_sql(buf);

	ut_a(graph);

	graph->trx = trx;
	trx->graph = NULL;
	/*对graph的执行*/
	graph->fork_type = QUE_FORK_MYSQL_INTERFACE;
	ut_a(thr = que_fork_start_command(graph, SESS_COMM_EXECUTE, 0));

	que_run_threads(thr);

	error = trx->error_state;

	que_graph_free(graph);

	if (error != DB_SUCCESS) {
		fprintf(stderr, "InnoDB: Foreign key constraint creation failed:\n"
			"InnoDB: internal error number %lu\n", error);

		if (error == DB_DUPLICATE_KEY) {
			fprintf(stderr, "InnoDB: Duplicate key error in system table %s index %s\n",
				((dict_index_t*)trx->error_info)->table_name,
				((dict_index_t*)trx->error_info)->name);

			fprintf(stderr, "%s\n", buf);

			fprintf(stderr,
				"InnoDB: Maybe the internal data dictionary of InnoDB is\n"
				"InnoDB: out-of-sync from the .frm files of your tables.\n"
				"InnoDB: See section 15.1 Troubleshooting data dictionary operations\n"
				"InnoDB: at http://www.innodb.com/ibman.html\n");
		}

		return(error);
	}

	foreign = UT_LIST_GET_NEXT(foreign_list, foreign);

	goto loop;
}










#include "dict0boot.h"
#include "dict0crea.h"
#include "btr0btr.h"
#include "dict0load.h"
#include "dict0load.h"
#include "trx0trx.h"
#include "srv0srv.h"
#include "ibuf0ibuf.h"
#include "buf0flu.h"
#include "log0recv.h"
#include "os0file.h"

void	dict_hdr_flush_row_id(void);

/*分配一个新的row id值*/
UNIV_INLINE dulint dict_sys_get_new_row_id()
{
	dulint	id;

	mutex_enter(&(dict_sys->mutex));
	id = dict_sys->row_id;
	if (0 == (ut_dulint_get_low(id) % DICT_HDR_ROW_ID_WRITE_MARGIN))
		dict_hdr_flush_row_id();

	UT_DULINT_INC(dict_sys->row_id);

	mutex_exit(&(dict_sys->mutex));

	return(id);
}

/*从记录的field中读取row id*/
UNIV_INLINE dulint dict_sys_read_row_id(byte* field)
{
	ut_ad(DATA_ROW_ID_LEN == 6);
	return mach_read_from_6(field);
}

/*将row id写入到field中*/
UNIV_INLINE void dict_sys_write_row_id(byte* field, dulint row_id)
{
	ut_ad(DATA_ROW_ID_LEN == 6);
	mach_write_to_6(field, row_id);
}

/*获得数据字典的头页*/
dict_hdr_t* dict_hdr_get(mtr_t* mtr)
{
	dict_hdr_t* header;

	ut_ad(mtr);

	header = DICT_HDR + buf_page_get(DICT_HDR_SPACE, DICT_HDR_PAGE_NO, RW_X_LATCH, mtr);
	buf_page_dbg_add_level(header, SYNC_DICT_HEADER);

	return header;
}

/*获得一个table或者索引或者索引树的新ID值，根据type对应的偏移而确定*/
dulint dict_hdr_get_new_id(ulint type)
{
	dict_hdr_t*	dict_hdr;
	dulint		id;
	mtr_t		mtr;

	ut_ad((type == DICT_HDR_TABLE_ID) || (type == DICT_HDR_INDEX_ID) || (type == DICT_HDR_MIX_ID));

	mtr_start(&mtr);
	
	dict_hdr = dict_hdr_get(&mtr);
	/*从头页中取出上一次的id值*/
	id = mtr_read_dulint(dict_hdr + type, MLOG_8BYTES, &mtr);
	if(0 == ut_dulint_cmp(id, ut_dulint_max))
		printf("Max id \n");
	/*id + 1*/
	id = ut_dulint_add(id, 1);
	/*保存这一次的值到dict header page中*/
	mlog_write_dulint(dict_hdr + type, id, MLOG_8BYTES, &mtr); 

	mtr_commit(&mtr);
	return id;
}

/*将dict_sys中的row id刷新到dict header page的DICT_HDR_ROW_ID当中*/
void dict_hdr_flush_row_id()
{
	dict_hdr_t*	dict_hdr;
	dulint		id;
	mtr_t		mtr;

	ut_ad(mutex_own(&(dict_sys->mutex)));
	
	id = dict_sys->row_id;
	mtr_start(&mtr);
	dict_hdr = dict_hdr_get(&mtr);
	mlog_write_dulint(dict_hdr + DICT_HDR_ROW_ID, id, MLOG_8BYTES, &mtr); 
	mtr_commit(&mtr);
}

/*创建一个dict header page,在数据库创建的时候调用*/
static ibool dict_hdr_create(mtr_t* mtr)
{
	dict_hdr_t*	dict_header;
	ulint		hdr_page_no;
	ulint		root_page_no;
	page_t*		page;

	ut_ad(mtr);
	/*为dict header创建一个page,并分配一个file segment*/
	page = fseg_create(DICT_HDR_SPACE, 0, DICT_HDR + DICT_HDR_FSEG_HEADER, mtr);

	hdr_page_no = buf_frame_get_page_no(page);
	ut_a(DICT_HDR_PAGE_NO == hdr_page_no);

	dict_header = dict_hdr_get(mtr);
	/*row id*/
	mlog_write_dulint(dict_header + DICT_HDR_ROW_ID, ut_dulint_create(0, DICT_HDR_FIRST_ID), MLOG_8BYTES, mtr);
	/*table id*/
	mlog_write_dulint(dict_header + DICT_HDR_TABLE_ID, ut_dulint_create(0, DICT_HDR_FIRST_ID),MLOG_8BYTES, mtr);
	/*index id*/
	mlog_write_dulint(dict_header + DICT_HDR_INDEX_ID, ut_dulint_create(0, DICT_HDR_FIRST_ID), MLOG_8BYTES, mtr);
	/*mix id*/
	mlog_write_dulint(dict_header + DICT_HDR_MIX_ID, ut_dulint_create(0, DICT_HDR_FIRST_ID), MLOG_8BYTES, mtr);

	/*为聚集索引创建一个btree的root page*/
	root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_TABLES_ID, mtr);
	if(root_page_no == FIL_NULL)
		return FALSE;
	mlog_write_ulint(dict_header + DICT_HDR_TABLES, root_page_no, MLOG_4BYTES, mtr);

	/*创建唯一索引树的root page*/
	root_page_no = btr_create(DICT_UNIQUE, DICT_HDR_SPACE, DICT_TABLE_IDS_ID, mtr);
	if(root_page_no == FIL_NULL)
		return FALSE;
	mlog_write_ulint(dict_header + DICT_HDR_TABLE_IDS, root_page_no, MLOG_4BYTES, mtr);

	root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_COLUMNS_ID, mtr);
	if (root_page_no == FIL_NULL)
		return(FALSE);
	mlog_write_ulint(dict_header + DICT_HDR_COLUMNS, root_page_no, MLOG_4BYTES, mtr);

	root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_INDEXES_ID, mtr);
	if (root_page_no == FIL_NULL)
		return(FALSE);
	mlog_write_ulint(dict_header + DICT_HDR_INDEXES, root_page_no, MLOG_4BYTES, mtr);

	root_page_no = btr_create(DICT_CLUSTERED | DICT_UNIQUE, DICT_HDR_SPACE, DICT_FIELDS_ID, mtr);
	if (root_page_no == FIL_NULL)
		return(FALSE);
	mlog_write_ulint(dict_header + DICT_HDR_FIELDS, root_page_no, MLOG_4BYTES, mtr);
}

/*********************************************************************
Initializes the data dictionary memory structures when the database is
started. This function is also called when the data dictionary is created. */
void dict_boot()
{
	dict_table_t*	table;
	dict_index_t*	index;
	dict_hdr_t*		dict_hdr;
	mtr_t			mtr;

	mtr_start(&mtr);

	dict_init();

	mutex_enter(&(dict_sys->mutex));

	dict_hdr = dict_hdr_get(&mtr);
	/*有可能数据库服务器在关闭的时候，dict header page并没有刷到磁盘上，这个时候只有通过redo log重演来进行恢复，而在这时我们设置一个至少
	  大于dict header page中row id 256的值作为新的row id,这样可以防止重复*/
	dict_sys->row_id = ut_dulint_add(
		ut_dulint_align_up(mtr_read_dulint(dict_hdr + DICT_HDR_ROW_ID, MLOG_8BYTES, &mtr), DICT_HDR_ROW_ID_WRITE_MARGIN), DICT_HDR_ROW_ID_WRITE_MARGIN);
	/*建立一个SYS_TABLES表*/
	table = dict_mem_table_create("SYS_TABLES", DICT_HDR_SPACE, 8);
	dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "ID", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "N_COLS", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "TYPE", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "MIX_ID", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "MIX_LEN", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "CLUSTER_NAME", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "SPACE", DATA_INT, 0, 4, 0);

	table->id = DICT_TABLES_ID;

	dict_table_add_to_cache(table);
	dict_sys->sys_tables = table;

	/*为SYS_TABLES建立一个基于CLUST_IND列的聚集索引*/
	index = dict_mem_index_create("SYS_TABLES", "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 1);
	dict_mem_index_add_field(index, "NAME", 0);
	index->page_no = mtr_read_ulint(dict_hdr + DICT_HDR_TABLES, MLOG_4BYTES, &mtr);
	index->id = DICT_TABLES_ID;
	ut_a(dict_index_add_to_cache(table, index));

	/*建立一个ID_IND的唯一索引*/;
	index = dict_mem_index_create("SYS_TABLES", "ID_IND", DICT_HDR_SPACE, DICT_UNIQUE, 1);
	dict_mem_index_add_field(index, "ID", 0);
	index->page_no = mtr_read_ulint(dict_hdr + DICT_HDR_TABLE_IDS, MLOG_4BYTES, &mtr);
	index->id = DICT_TABLE_IDS_ID;
	ut_a(dict_index_add_to_cache(table, index));

	/*建立SYS_COLUMNS*/
	table = dict_mem_table_create("SYS_COLUMNS", DICT_HDR_SPACE, 7);
	dict_mem_table_add_col(table, "TABLE_ID", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "POS", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "MTYPE", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "PRTYPE", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "LEN", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "PREC", DATA_INT, 0, 4, 0);

	table->id = DICT_COLUMNS_ID;
	dict_table_add_to_cache(table);
	dict_sys->sys_columns = table;

	index = dict_mem_index_create("SYS_COLUMNS", "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);
	dict_mem_index_add_field(index, "TABLE_ID", 0);
	dict_mem_index_add_field(index, "POS", 0);

	index->page_no = mtr_read_ulint(dict_hdr + DICT_HDR_COLUMNS, MLOG_4BYTES, &mtr);
	index->id = DICT_COLUMNS_ID;
	ut_a(dict_index_add_to_cache(table, index));
	
	/*建立SYS_INDEXS表*/
	table = dict_mem_table_create("SYS_INDEXES", DICT_HDR_SPACE, 7);
	dict_mem_table_add_col(table, "TABLE_ID", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "ID", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "NAME", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "N_FIELDS", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "TYPE", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "SPACE", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "PAGE_NO", DATA_INT, 0, 4, 0);

	/* The '+ 2' below comes from the 2 system fields */
	ut_ad(DICT_SYS_INDEXES_PAGE_NO_FIELD == 6 + 2);
	ut_ad(DICT_SYS_INDEXES_SPACE_NO_FIELD == 5 + 2); 

	table->id = DICT_INDEXES_ID;
	dict_table_add_to_cache(table);
	dict_sys->sys_indexes = table;

	index = dict_mem_index_create("SYS_INDEXES", "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);
	dict_mem_index_add_field(index, "TABLE_ID", 0);
	dict_mem_index_add_field(index, "ID", 0);

	index->page_no = mtr_read_ulint(dict_hdr + DICT_HDR_INDEXES, MLOG_4BYTES, &mtr);
	index->id = DICT_INDEXES_ID;
	ut_a(dict_index_add_to_cache(table, index));

	/*SYS_FIELDS表*/
	table = dict_mem_table_create("SYS_FIELDS", DICT_HDR_SPACE, 3);

	dict_mem_table_add_col(table, "INDEX_ID", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "POS", DATA_INT, 0, 4, 0);
	dict_mem_table_add_col(table, "COL_NAME", DATA_BINARY, 0, 0, 0);

	table->id = DICT_FIELDS_ID;
	dict_table_add_to_cache(table);
	dict_sys->sys_fields = table;

	index = dict_mem_index_create("SYS_FIELDS", "CLUST_IND", DICT_HDR_SPACE, DICT_UNIQUE | DICT_CLUSTERED, 2);

	dict_mem_index_add_field(index, "INDEX_ID", 0);
	dict_mem_index_add_field(index, "POS", 0);

	index->page_no = mtr_read_ulint(dict_hdr + DICT_HDR_FIELDS, MLOG_4BYTES, &mtr);
	index->id = DICT_FIELDS_ID;
	ut_a(dict_index_add_to_cache(table, index));

	mtr_commit(&mtr);
	/*初始化ibuffer 表空间*/
	ibuf_init_at_db_start();

	dict_load_sys_table(dict_sys->sys_tables);
	dict_load_sys_table(dict_sys->sys_columns);
	dict_load_sys_table(dict_sys->sys_indexes);
	dict_load_sys_table(dict_sys->sys_fields);

	mutex_exit(&(dict_sys->mutex));
}

static void dict_insert_initial_data()
{

}

/*创建并初始化数据字典,数据库创建或者启动时调用*/
void dict_create()
{
	mtr_t mtr;

	mtr_start(&mtr);
	dict_hdr_create(&mtr);
	mtr_commit(&mtr);

	dict_boot();
	dict_insert_initial_data();

	sync_order_checks_on = TRUE;
}



#ifndef dict0mem_h_
#define dict0mem_h_

#include "univ.h"
#include "dict0types.h"
#include "data0type.h"
#include "data0data.h"
#include "mem0mem.h"
#include "rem0types.h"
#include "btr0types.h"
#include "ut0mem.h"
#include "ut0lst.h"
#include "ut0rnd.h"
#include "ut0byte.h"
#include "sync0rw.h"
#include "lock0types.h"
#include "hash0hash.h"
#include "que0types.h"

/*索引类型*/

#define DICT_CLUSTERED			1			/*聚集索引*/
#define DICT_UNIQUE				2			/*唯一索引*/
#define DICT_UNIVERSAL			4			/*普通索引*/
#define DICT_IBUF				8			/*IBUF B tree*/

#define DICT_DESCEND			1

/* Types for a table object */
#define DICT_TABLE_ORDINARY		1
#define	DICT_TABLE_CLUSTER_MEMBER	2
#define	DICT_TABLE_CLUSTER		3

#define	DICT_TREE_MAGIC_N	7545676

struct dict_col_struct
{
	hash_node_t					hash;				/*哈希节点*/
	ulint						ind;				/*该列在表中的偏移量*/
	ulint						clust_pos;			/*该列在聚集索引中的位置*/
	ulint						ord_part;			/*该列作为索引排序的次数*/
	char*						name;				/*列名*/
	dtype_t						type;				/*列的数据类型*/
	dict_table_t*				table;				/*所属表对象*/
	ulint						aux;				/*辅助字段*/
};

struct dict_field_struct
{
	dict_col_t*					col;				/*索引不包含的列？*/
	char*						name;				/*列名*/
	ulint						order;				/*排序标识*/
};

/*索引树的数据结构定义*/
struct dict_tree_struct
{
	ulint						type;				/*树的类型*/
	dulint						id;
	ulint						space;
	ulint						page;
	byte						pad[64];
	rw_lock_t					lock;
	ulint						mem_fix;
	UT_LIST_BASE_NODE_T(dict_index_t)	tree_indexs;
	ulint						magic_n;
};

/*索引定义的数据结构*/
struct dict_index_struct
{
	dulint						id;					/*索引ID*/
	mem_heap_t*					heap;				
	ulint						type;				/*索引类型*/

	char*						name;				/*索引名称*/
	char*						table_name;			/*所在表名*/
	dict_table_t*				table;				/*所在表对象*/
	ulint						space;				/*索引树存放的表空间ID*/
	ulint						page_no;			/*索引树root节点对应的page no*/
	ulint						trx_id_offset;		/**/
	ulint						n_user_defined_cols;/*该索引定义的列数*/
	ulint						n_uniq;				/*能够确定唯一索引项所需的列数*/
	ulint						n_def;
	ulint						n_fields;			/*索引列数组长度*/
	dict_field_t*				fields;				/*索引列数组*/
	UT_LIST_NODE_T(dict_index_t) indexs;			/*所在表中所有索引对象的节点列表*/
	UT_LIST_NODE_T(dict_index_t) tree_indexs;		/*关联对应索引树中保存的所有索引树的节点列表*/

	dict_tree_t*				tree;				/*对应的索引树*/
	ibool						cached;
	btr_search_t*				search_info;		/*自适应HASH索引信息*/
	ib_longlong*				stat_n_diff_key_vals; /*该索引不同键值的数量（大概）*/
	ulint						stat_index_size;	/*该索引占用的page数*/
	ulint						stat_n_leaf_pages;	/*该索引的索引树中叶子节点的数目*/
	ulint						magic_n;
};

/*外键约束的数据结构*/
struct dict_foreign_struct
{
	mem_heap_t*					heap;				/*外键约束heap*/
	char*						id;					/*外键约束的ID字符串*/
	char*						type;				/* 0 or DICT_FOREIGN_ON_DELETE_CASCADE or DICT_FOREIGN_ON_DELETE_SET_NULL */
	char*						foreign_table_name;	/*外键从表名*/
	dict_table_t*				foreign_table;		/*外键从表对象*/
	char**						foreign_col_names;	/*外键中的从表列名*/
	char*						referenced_table_name;/*外键主表名*/
	dict_table_t*				referenced_table;	/*外键主表对象*/
	char**						referenced_col_names;/*外键中的主表列名*/
	ulint						n_fields;			/*用于定义该外键约束的索引首列个数*/
	dict_index_t*				foreign_index;		/*该外键中的从表列所在表中对应的索引对象*/
	dict_index_t*				referenced_index;	/*该外键中的主表列所在表中对应的索引对象*/
	UT_LIST_NODE_T(dict_foreign_t) foreign_list;
	UT_LIST_NODE_T(dict_foreign_t) referenced_list;
};

#define DICT_FOREIGN_ON_DELETE_CASCADE	1
#define DICT_FOREIGN_ON_DELETE_SET_NULL	2

#define	DICT_INDEX_MAGIC_N	76789786
/*数据库表结构定义*/
struct dict_table_struct
{
	dulint		id;					/*表ID*/
	ulint		type;				/*表类型*/
	mem_heap_t*	heap;				/* memory heap */
	char*		name;				/*表名*/
	ulint		space;				/* 聚集索引存储的表空间ID */
	hash_node_t	name_hash;			/* hash chain node */
	hash_node_t	id_hash;			/* hash chain node */
	ulint		n_def;				/* number of columns defined so far */
	ulint		n_cols;				/* number of columns */
	dict_col_t*	cols;				/* array of column descriptions */
	UT_LIST_BASE_NODE_T(dict_index_t) indexes; /*该表所有索引对象的列表*/
	UT_LIST_BASE_NODE_T(dict_foreign_t) foreign_list; /*该表用于分别连接该表从表的外键对象列表*/
	UT_LIST_BASE_NODE_T(dict_foreign_t) referenced_list;/*该表用于分别连接该表主表的外键对象列表 */
	UT_LIST_NODE_T(dict_table_t) table_LRU;		/* node of the LRU list of tables */
	ulint		mem_fix;			/*用于记录fix的计数器*/
	ulint		n_mysql_handles_opened; /*用于技术该表被多少MYSQL句柄打开*/
	ulint		n_foreign_key_checks_running; /**/
	ibool		cached;				/*表对象是否已经在数据字典缓冲中*/
	lock_t*		auto_inc_lock;		/*表自增长mutex*/
	UT_LIST_BASE_NODE_T(lock_t) locks; /*用于该表当前所有锁列表*/

	dulint		mix_id;					/* */
	ulint		mix_len;				/**/
	ulint		mix_id_len;
	byte		mix_id_buf[12];			/**/
	char*		cluster_name;			/**/

	ibool		does_not_fit_in_memory; /**/

	ib_longlong	stat_n_rows;			/*该表的大概行数*/
	ulint		stat_clustered_index_size; /*该表聚集索引占用的页数*/
	ulint		stat_sum_of_other_index_sizes;	/*该表非聚集索引占用的页数*/
	ibool           stat_initialized;	/*统计字段是否被初始化*/
	ulint		stat_modified_counter;	/**/

	mutex_t		autoinc_mutex;			/**/
	ibool		autoinc_inited;			/*自增长ID是否初始化了*/
	ib_longlong	autoinc;				/*自增长ID*/	
	ulint		magic_n;				/* magic number */
};

#define	DICT_TABLE_MAGIC_N	76333786

/*存储过程结构定义*/
struct dict_proc_struct
{
	mem_heap_t*			heap;
	char*				name;
	char*				sql_string;
	hash_node_t			name_hash;
	UT_LIST_BASE_NODE_T(que_fork_t) graphs;
	ulint				mem_fix;
};

dict_table_t*			dict_mem_table_create(char*	name, ulint space, ulint n_cols);

dict_cluster_t*			dict_mem_cluster_create(char* name, ulint space, ulint n_cols, ulint mix_len);

void					dict_mem_table_make_cluster_member(dict_table_t* table, char* cluster_name);

void					dict_mem_table_add_col(dict_table_t* table, char* name, ulint mtype, ulint prtype, ulint len, ulint prec);

dict_index_t*			dict_mem_index_create(char* table_name, char* index_name, ulint space, ulint type, ulint n_fields);

void					dict_mem_index_add_field(dict_index_t* index, char* name, ulint order);

void					dict_mem_index_free(dict_index_t* index);

dict_foreign_t*			dict_mem_foreign_create();

dict_proc_t*			dict_mem_procedure_create(char* name, char* sql_string, que_fork_t* graph);

#endif





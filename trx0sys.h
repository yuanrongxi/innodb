#ifndef __trx0sys_h_
#define __trx0sys_h_

#include "univ.h"

#include "trx0types.h"
#include "mtr0mtr.h"
#include "mtr0log.h"
#include "ut0byte.h"
#include "mem0mem.h"
#include "sync0sync.h"
#include "ut0lst.h"
#include "buf0buf.h"
#include "fil0fil.h"
#include "fut0lst.h"
#include "fsp0fsp.h"
#include "read0types.h"

/***********************MACRO********************************************/
/*默认的rollback segment id的值*/
#define TRX_SYS_SYSTEM_RSEG_ID			0

/*事务表空间ID*/
#define TRX_SYS_SPACE					0

#define TRX_SYS_PAGE_NO					FSP_TRX_SYS_PAGE_NO

#define TRX_SYS							FSEG_PAGE_DATA

/*****trx_sys_t在磁盘上的偏移位置**********/
#define TRX_SYS_TRX_ID_STORE			0
#define	TRX_SYS_FSEG_HEADER				8
#define TRX_SYS_RSEGS					8 + FSEG_HEADER_SIZE

/***************一些有关trx_sys_t的常量****/
#define TRX_SYS_N_RSEGS					256

#define TRX_SYS_MYSQL_LOG_NAME_LEN		512
#define TRX_SYS_MYSQL_LOG_MAGIC_N		873422344

#define TRX_SYS_MYSQL_MASTER_LOG_INFO	(UNIV_PAGE_SIZE - 2000)
#define TRX_SYS_MYSQL_LOG_INFO			(UNIV_PAGE_SIZE - 1000)
#define	TRX_SYS_MYSQL_LOG_MAGIC_N_FLD	0
#define TRX_SYS_MYSQL_LOG_OFFSET_HIGH	4
#define TRX_SYS_MYSQL_LOG_OFFSET_LOW	8
#define TRX_SYS_MYSQL_LOG_NAME			12

/*doublewrite信息存在事务段头页的末尾200字节中*/
#define TRX_SYS_DOUBLEWRITE				(UNIV_PAGE_SIZE - 200)
#define TRX_SYS_DOUBLEWRITE_FSEG 		0
#define TRX_SYS_DOUBLEWRITE_MAGIC		FSEG_HEADER_SIZE
#define TRX_SYS_DOUBLEWRITE_BLOCK1		(4 + FSEG_HEADER_SIZE)
#define TRX_SYS_DOUBLEWRITE_BLOCK2		(8 + FSEG_HEADER_SIZE)
#define TRX_SYS_DOUBLEWRITE_REPEAT		12

#define TRX_SYS_DOUBLEWRITE_MAGIC_N		536853855
/*doublewrite有两块，1块为1M，64个page*/
#define TRX_SYS_DOUBLEWRITE_BLOCK_SIZE	FSP_EXTENT_SIZE

#define TRX_SYS_TRX_ID_WRITE_MARGIN	256

struct trx_doublewrite_struct
{
	mutex_t		mutex;
	ulint		block1;			/*第一个doublewirte block的起始页的page_no*/
	ulint		block2;			/*第二个doublewirte block的起始页的page_no*/
	ulint		first_free;		/*write buffer的第一个空闲位置，以page_size为单位计数偏移*/
	byte*		write_buf;
	byte*		write_buf_unaligned;

	buf_block_t** buf_block_arr;
};

struct trx_sys_struct
{
	dulint						max_trx_id;

	UT_LIST_BASE_NODE_T(trx_t)	trx_list;				/*正在执行或者commit的事务列表*/
	UT_LIST_BASE_NODE_T(trx_t)	mysql_trx_list;			/*被上层mysql创建的事务列表*/
	UT_LIST_BASE_NODE_T(trx_rseg_t) rseg_list;			/*rollback segment list*/
	UT_LIST_BASE_NODE_T(read_view_t) view_list;			/**/

	trx_rseg_t*					latest_rseg;
	trx_rseg_t*					rseg_array[TRX_SYS_N_RSEGS];
};

/****************全局变量申明***********/
extern char					trx_sys_mysql_master_log_name[];
extern ib_longlong			trx_sys_mysql_master_log_pos;

/*全局的事务管理器*/
extern trx_sys_t*			trx_sys;
/*全局双写管理器*/
extern trx_doublewrite_t*	trx_doublewrite;

/****************函数*******************/
void						trx_sys_create_doublewrite_buf();

void						trx_sys_doublewrite_restore_corrupt_pages();

ibool						trx_doublewrite_page_inside(ulint page_no);

UNIV_INLINE ibool			trx_sys_hdr_page(ulint space, ulint page_no);

void						trx_sys_init_at_db_start();

void						trx_sys_create();

ulint						trx_sysf_rseg_find_free(mtr_t* mtr);

UNIV_INLINE trx_rseg_t*		trx_sys_get_nth_rseg(trx_sys_t* sys, ulint n);

UNIV_INLINE void			trx_sys_set_nth_rseg(trx_sys_t* sys, ulint n, trx_rseg_t* rseg);

UNIV_INLINE trx_sysf_t*		trx_sysf_get(mtr_t* mtr);

UNIV_INLINE ulint			trx_sysf_rseg_get_space(trx_sysf_t* sys_header, ulint i, mtr_t* mtr);

UNIV_INLINE ulint			trx_sysf_rseg_get_page_no(trx_sysf_t* sys_header, ulint i, mtr_t* mtr);

UNIV_INLINE void			trx_sysf_rseg_set_space(trx_sysf_t* sys_header, ulint i, ulint space, mtr_t* mtr);

UNIV_INLINE void			trx_sysf_rseg_set_page_no(trx_sysf_t* sys_header, ulint i, ulint page_no, mtr_t* mtr);

UNIV_INLINE dulint			trx_sys_get_new_trx_id();

UNIV_INLINE dulint			trx_sys_get_new_trx_no();

UNIV_INLINE void			trx_write_trx_id(byte* ptr, dulint id);

UNIV_INLINE dulint			trx_read_trx_id(byte* ptr);

UNIV_INLINE trx_t*			trx_get_on_id(dulint trx_id);

UNIV_INLINE dulint			trx_list_get_min_trx_id();

UNIV_INLINE ibool			trx_is_active(dulint trx_id);

ibool						trx_in_trx_list(trx_t* in_trx);

void						trx_sys_update_mysql_binlog_offset(char* file_name, ib_longlong offset, ulint field, mtr_t* mtr);

void						trx_sys_print_mysql_binlog_offset();

void						trx_sys_print_mysql_binlog_offset_from_page(byte* page);

#include "trx0sys.inl"

#endif






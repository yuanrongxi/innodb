#ifndef __trx0types_h_
#define __trx0types_h_

#include "lock0types.h"
#include "ut0byte.h"

/*事务的内存对象定义*/
typedef struct trx_struct				trx_t;
typedef struct trx_sys_struct			trx_sys_t;
typedef struct trx_doublewrite_struct	trx_doublewrite_t;
typedef struct trx_sig_struct			trx_sig_t;
typedef struct trx_rseg_struct			trx_rseg_t;
typedef struct trx_undo_struct			trx_undo_t;
typedef struct trx_undo_arr_struct		trx_undo_arr_t;
typedef struct trx_undo_inf_struct		trx_undo_inf_t;
typedef struct trx_purge_struct			trx_purge_t;
typedef struct roll_node_struct			roll_node_t;
typedef struct commit_node_struct		commit_node_t;


typedef struct trx_savept_struct
{
	dulint least_undo_no;
}trx_savept_t;

/*事务的文件对象定义*/
typedef byte			trx_sysf_t;
typedef byte			trx_rsegf_t;
typedef byte			trx_usegf_t;
typedef byte			trx_ulogf_t;
typedef byte			trx_upagef_t;

/*undo log record*/
typedef byte			trx_undo_rec_t;

#endif





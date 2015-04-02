#ifndef __read0read_h_
#define __read0read_h_

#include "univ.h"
#include "ut0bype.h"
#include "ut0lst.h"
#include "trx0trx.h"

typedef struct read_view_struct read_view_t;
/*事务的可见依赖结构*/
typedef struct read_view_struct
{
	ibool			can_be_too_old;					/*TRUE表示必须进行purge old version, read view还是能继续访问数据，但又可能会DB_MISSING_HISTORY错误*/
	dulint			low_limit_no;					
	dulint			low_limit_id;					/*trx_ids的下限值*/
	dulint			up_limit_id;					/*trx ids的上限值*/
	ulint			n_trx_ids;
	dulint*			trx_ids;						
	trx_t*			creator;						/*归属的事务句柄*/

	UT_LIST_NODE_T(read_view_t) view_list;			/*为了以链表形式保存在trx_sys->view_list当中定义的链表前后关系*/
}read_view_t;


read_view_t*				read_view_open_now(trx_t* cr_trx, mem_heap_t* heap);

read_view_t*				read_view_oldest_copy_or_open_new(trx_t* cr_trx, mem_heap_t* heap);

void						read_view_close(read_view_t* view);

UNIV_INLINE	ibool			read_view_sees_trx_id(read_view_t* view, dulint trx_id);

void						read_view_printf(read_view_t* view);

#include "read0read.inl"

#endif

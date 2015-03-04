#ifndef __trx0rseg_h_
#define __trx0rseg_h_

#include "univ.h"
#include "trx0types.h"
#include "trx0sys.h"

/* Undo log segment slot in a rollback segment header */
#define TRX_RSEG_SLOT_PAGE_NO		0			/*undo log头页的页序号*/

#define TRX_RSEG_SLOT_SIZE			4			/*slot size*/

#define TRX_RSEG				FSEG_PAGE_DATA

#define	TRX_RSEG_MAX_SIZE			0

#define TRX_RSEG_HISTORY_SIZE		4

#define TRX_RSEG_HISTORY			8

#define TRX_RSEG_FSEG_HEADER		(8 + FLST_BASE_NODE_SIZE)

#define TRX_RSEG_UNDO_SLOTS			(8 + FLST_BASE_NODE_SIZE + FSEG_HEADER_SIZE)

#define TRX_RSEG_N_SLOTS			1024
/*单个rollsegment段能支持的最大事务数？*/
#define TRX_RSEG_MAX_N_TRXS			(TRX_RSEG_N_SLOTS / 2) 

/*定义trx_rollsegment*/
struct trx_rseg_struct
{
	ulint							id;			/*roll segment id*/
	mutex_t							mutex;		/*对roll segment所有数据的互斥保护*/

	ulint							space;
	ulint							page_no;	/*rollsegment 头页的page_no*/

	ulint							max_size;	/*rollsegment最大可以占用的page数量*/
	ulint							curr_size;	/*rollsegment已经占用的page数量*/

	UT_LIST_BASE_NODE_T(trx_undo_t) update_undo_list;
	UT_LIST_BASE_NODE_T(trx_undo_t) update_undo_cached;

	UT_LIST_BASE_NODE_T(trx_undo_t) insert_undo_list;
	UT_LIST_BASE_NODE_T(trx_undo_t) insert_undo_cached;

	ulint							last_page_no;
	ulint							last_offset;
	dulint							last_trx_no;
	ibool							last_del_marks;

	UT_LIST_NODE_T(trx_rseg_t)		rseg_list;
};

/*rollsegment操作控制函数*/

UNIV_INLINE trx_rsegf_t*			trx_rsegf_get(ulint space, ulint page_no, mtr_t* mtr);

UNIV_INLINE trx_rsegf_t*			trx_rsegf_get_new(ulint space, ulint page_no, mtr_t* mtr);

UNIV_INLINE ulint					trx_rsegf_get_nth_undo(trx_rsegf_t* rsegf, ulint n, mtr_t* mtr);

UNIV_INLINE void					trx_rsegf_set_nth_undo(trx_rsegf_t* rsegf, ulint n, ulint page_no, mtr_t* mtr);

UNIV_INLINE ulint					trx_rsegf_undo_find_free(trx_rsegf_t* rsegf, mtr_t* mtr);

trx_rseg_t*							trx_rseg_get_on_id(ulint id);

ulint								trx_rseg_header_create(ulint space, ulint max_size, ulint* slot_no, mtr_t* mtr);

void								trx_rseg_list_and_array_init(trx_sysf_t* sys_header, mtr_t* mtr);

trx_rseg_t*							trx_rseg_create(ulint space, ulint max_size, ulint* id, mtr_t* mtr);

#include "trx0rseg.inl"
#endif




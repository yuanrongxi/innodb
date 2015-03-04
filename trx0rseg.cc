#include "trx0rseg.h"
#include "trx0undo.h"
#include "fut0lst.h"
#include "srv0srv.h"
#include "trx0purge.h"

/*通过rollback segment id查找rollback segment 对象*/
trx_rseg_t* trx_rseg_get_on_id)(ulint id)
{
	trx_rseg_t* rseg;

	rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);
	ut_ad(rseg);

	while(rseg->id != id){
		rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
		ut_ad(rseg);
	}

	return rseg;
}

/*创建和分配一个rollback segment header page页*/
ulint trx_rseg_header_create(ulint space, ulint max_size, ulint* slot_no, mtr_t* mtr)
{
	ulint		page_no;
	trx_rsegf_t*	rsegf;
	trx_sysf_t*	sys_header;
	ulint		i;
	page_t*		page;

	ut_ad(mtr);
	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(mtr_memo_contains(mtr, fil_space_get_latch(space), MTR_MEMO_X_LOCK));

	sys_header = trx_sysf_get(mtr);
	*slot_no = trx_sysf_rseg_find_free(mtr);

	if(*slot_no == ULINT_UNDEFINED)
		return FIL_NULL;

	/*获得一个rollback segment header page*/
	page = fseg_create(space, 0, TRX_RSEG + TRX_RSEG_FSEG_HEADER, mtr);
	if(page == NULL)
		return FIL_NULL;

	buf_page_dbg_add_level(page, SYNC_RSEG_HEADER_NEW);
	page_no = buf_frame_get_page_no(page);

	rsegf = trx_rsegf_get_new(space, page_no, mtr);
	/*设置rollback segment最大可以占用的page数*/
	mlog_write_ulint(rsegf + TRX_RSEG_MAX_SIZE, max_size, MLOG_4BYTES, mtr);
	mlog_write_ulint(rsegf + TRX_RSEG_HISTORY_SIZE, 0, MLOG_4BYTES, mtr);
	/*初始化slots*/
	for(i = 0; i < TRX_RSEG_N_SLOTS; i++)
		trx_rsegf_set_nth_undo(rsegf, i, FIL_NULL, mtr);

	/*设置trx_sys中的rollback segment定位关系和位置*/
	trx_sysf_rseg_set_space(sys_header, *slot_no, space, mtr);	
	trx_sysf_rseg_set_page_no(sys_header, *slot_no, page_no, mtr);
}


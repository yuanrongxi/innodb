#include "srv0srv.h"
#include "trx0trx.h"
#include "data0type.h"

typedef byte trx_sysf_rseg_t;

#define TRX_SYS_RSEG_SPACE		0
#define TRX_SYS_RSEG_PAGE_NO	4
#define TRX_SYS_RSEG_SLOT_SIZE	8

/*将max_trx_id写到trx_system header对应的磁盘空间文件中*/
void trx_sys_flush_max_trx_id();

/*判断（space, page_no）是否是trx_sys header page*/
UNIV_INLINE ibool trx_sys_hdr_page(ulint space, ulint page_no)
{
	if(space == TRX_SYS_SPACE && page_no == TRX_SYS_PAGE_NO)
		return TRUE;

	return FALSE;
}

/*将rseg设置在sys的第n个槽位上*/
UNIV_INLINE void trx_sys_set_nth_rseg(trx_sys_t* sys, ulint n, trx_rseg_t* rseg)
{
	ut_ad(n < TRX_SYS_N_RSEGS);

	sys->rseg_array[n] = rseg;
}

/*获取trx sys的header信息指针，并对其加上x-latch进行控制*/
UNIV_INLINE trx_sysf_t* trx_sysf_get(mtr_t* mtr)
{
	trx_sysf_t*	header;

	ut_ad(mtr);

	header = TRX_SYS + buf_page_get(TRX_SYS_SPACE, TRX_SYS_PAGE_NO, RW_X_LATCH, mtr);
	buf_page_dbg_add_level(header, SYNC_TRX_SYS_HEADER);

	return header;
}

/*从trx_sys header中获取第i个rollback segment对应的space*/
UNIV_INLINE ulint trx_sysf_rseg_get_space(trx_sysf_t* sys_header, ulint i, mtr_t* mtr)
{
	ut_ad(mutex_own(&(kernel_mutex)));
	ut_ad(sys_header);
	ut_ad(i < TRX_SYS_N_RSEGS);

	return(mtr_read_ulint(sys_header + TRX_SYS_RSEGS+ i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE, MLOG_4BYTES, mtr));
}
/*从trx_sys header中获取第i个rollback segment对应的page_no*/
UNIV_INLINE ulint trx_sysf_rseg_get_page_no(trx_sysf_t* sys_header, ulint i, mtr_t* mtr)
{
	ut_ad(mutex_own(&(kernel_mutex)));
	ut_ad(sys_header);
	ut_ad(i < TRX_SYS_N_RSEGS);

	return(mtr_read_ulint(sys_header + TRX_SYS_RSEGS+ i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_PAGE_NO, MLOG_4BYTES, mtr));
}

UNIV_INLINE void trx_sysf_rseg_set_space(trx_sysf_t* sys_header, ulint i, ulint space, mtr_t* mtr)
{
	ut_ad(mutex_own(&(kernel_mutex)));
	ut_ad(sys_header);
	ut_ad(i < TRX_SYS_N_RSEGS);

	mlog_write_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE, space, mtr);
}

UNIV_INLINE void trx_sysf_rseg_set_page_no(trx_sysf_t* sys_header, ulint i, ulint page_no, mtr_t* mtr)
{
	ut_ad(mutex_own(&(kernel_mutex)));
	ut_ad(sys_header);
	ut_ad(i < TRX_SYS_N_RSEGS);

	mlog_write_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE, space, mtr);
}

UNIV_INLINE void trx_write_trx_id(byte* ptr, dulint id)
{
	ut_ad(DATA_TRX_ID_LEN == 6);
	mach_write_to_6(ptr, id);
}

UNIV_INLINE dulint trx_read_trx_id(byte* ptr)
{
	ut_ad(DATA_TRX_ID_LEN == 6);
	return mach_read_from_6(ptr);
}

/*通过trx_id查找多对应激活的事务对象*/
UNIV_INLINE trx_t* trx_get_on_id(dulint trx_id)
{
	trx_t* trx;

	ut_ad(mutex_own(&kernel_mutex));

	trx = UT_LIST_GET_FIRST(trx_sys->trx_list);
	while(trx != NULL){
		if(ut_dulint_cmp(trx_id, trx->id) == 0)
			return trx;

		trx = UT_LIST_GET_NEXT(trx_list, trx);
	}
}

/*获得trx_list中最小的trx id,一般是最后一个trx的ID*/
UNIV_INLINE dulint trx_list_get_min_trx_id()
{
	trx_t*	trx;

	ut_ad(mutex_own(&(kernel_mutex)));

	trx = UT_LIST_GET_LAST(trx_sys->trx_list);

	if (trx == NULL)
		return trx_sys->max_trx_id;

	return trx->id;
}

/*判断一个trx_id对应的事务对象是否是被激活了*/
UNIV_INLINE ibool trx_is_active(dulint trx_id)
{
	trx_t*	trx;

	ut_ad(mutex_own(&(kernel_mutex)));

	if(ut_dulint_cmp(trx_id, trx_list_get_min_trx_id()) < 0) 
		return FALSE;

	trx = trx_get_on_id(trx_id);
	if(trx && trx->conc_state == TRX_ACTIVE) 
		return TRUE;

	return FALSE;
}

/*分配一个新的trx id*/
UNIV_INLINE dulint trx_sys_get_new_trx_id()
{
	dulint	id;

	ut_ad(mutex_own(&kernel_mutex));

	if(ut_dulint_get_low(trx_sys->max_trx_id ) % TRX_SYS_TRX_ID_WRITE_MARGIN == 0)
		trx_sys_flush_max_trx_id();

	id = trx_sys->max_trx_id;

	UT_DULINT_INC(trx_sys->max_trx_id);

	return id;
}

/*等同trx_sys_get_new_trx_id*/
UNIV_INLINE dulint trx_sys_get_new_trx_no()
{
	ut_ad(mutex_own(&kernel_mutex));

	return trx_sys_get_new_trx_id();
}


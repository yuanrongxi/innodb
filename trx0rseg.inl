#include "srv0srv.h"

/*获得一个rollback segment的头页缓冲区*/
UNIV_INLINE trx_rsegf_t* trx_rsegf_get(ulint space, ulint page_no, mtr_t* mtr)
{
	trx_rsegf_t* header = buf_page_get(space, page_no, RW_X_LATCH, mtr);
	buf_page_dbg_add_level(header, SYNC_RSEG_HEADER);
	
	return header;
}

/*获得一个最近的rollback segment的头页缓冲区*/
UNIV_INLINE trx_rsegf_t* trx_rsegf_get_new(ulint space, ulint page_no, mtr_t* mtr)
{
	trx_rsegf_t* header;

	header = TRX_RSEG + buf_page_get(space, page_no, RW_X_LATCH, mtr);
	buf_page_dbg_add_level(header, SYNC_RSEG_HEADER_NEW);

	return(header);
}

/*获得rollback segment第n个undo slot对应页的page_no*/
UNIV_INLINE ulint trx_rsegf_get_nth_undo(trx_rsegf_t* rsegf, ulint n, mtr_t* mtr)
{
	if(n >= TRX_RSEG_N_SLOTS){
		fprintf(stderr, "InnoDB: Error: trying to get slot %lu of rseg\n", n);
		ut_a(0);
	}

	return mtr_read_ulint(rsegf + TRX_RSEG_UNDO_SLOTS + n * TRX_RSEG_SLOT_SIZE, MLOG_4BYTES, mtr);
}

UNIV_INLINE void trx_rsegf_set_nth_undo(trx_rsegf_t* rsegf, ulint n, ulint page_no, mtr_t* mtr)
{
	if (n >= TRX_RSEG_N_SLOTS) {
		fprintf(stderr, "InnoDB: Error: trying to set slot %lu of rseg\n", n);
		ut_a(0);
	}

	mlog_write_ulint(rsegf + TRX_RSEG_UNDO_SLOTS + n * TRX_RSEG_SLOT_SIZE, page_no, MLOG_4BYTES, mtr);
}

/*从slots中查找一个空闲的slot位置*/
UNIV_INLINE ulint trx_rsegf_undo_find_free(trx_rsegf_t* rsegf, mtr_t* mtr)
{
	ulint i;
	ulint page_no;

	for(i = 0; i < TRX_RSEG_N_SLOTS; i ++){
		page_no = trx_rsegf_get_nth_undo(rsegf, i, mtr);
		if(page_no == FIL_NULL)
			return i;
	}

	return ULINT_UNDEFINED;
}



#include "data0type.h"

/*构建一个roll ptr,一个不重复的64整型数*/
UNIV_INLINE dulint trx_undo_build_roll_ptr(ibool is_insert, ulint rseg_id, ulint page_no, ulint offset)
{
	ut_ad(DATA_ROLL_PTR_LEN == 7);
	ut_ad(rseg_id < 128);

	return(ut_dulint_create(is_insert * 128 * 256 * 256 + rseg_id * 256 * 256 + (page_no / 256) / 256,
		(page_no % (256 * 256)) * 256 * 256 + offset));
}

/*根据roll ptr求is_insert rseg_id page_no offset*/
UNIV_INLINE void trx_undo_decode_roll_ptr(dulint roll_ptr, ibool* is_insert, ulint* rseg_id, ulint* page_no, ulint* offset)
{
	ulint	low;
	ulint	high;

	ut_ad(DATA_ROLL_PTR_LEN == 7);
	ut_ad(TRUE == 1);

	high = ut_dulint_get_high(roll_ptr);
	low = ut_dulint_get_low(roll_ptr);

	*offset = low % (256 * 256);

	*is_insert = high / (256 * 256 * 128);	/* TRUE == 1 */
	*rseg_id = (high / (256 * 256)) % 128;

	*page_no = (high % (256 * 256)) * 256 * 256 + (low / 256) / 256;
}

/*通过roll_ptr判断是否是insert undo*/
UNIV_INLINE ibool trx_undo_roll_ptr_is_insert(dulint roll_ptr)
{
	ulint high;

	ut_ad(DATA_ROLL_PTR_LEN == 7);
	ut_ad(TRUE == 1);

	high = ut_dulint_get_high(roll_ptr);

	return high / (256 * 256 * 128);
}

/*将roll ptr写入ptr缓冲区*/
UNIV_INLINE void trx_write_roll_ptr(byte* ptr, dulint roll_ptr)
{
	ut_ad(DATA_ROLL_PTR_LEN == 7);

	mach_write_to_7(ptr, roll_ptr);
}

/*从ptr缓冲区中读取一个roll_ptr*/
UNIV_INLINE dulint trx_read_roll_ptr(byte* ptr)
{
	ut_ad(DATA_ROLL_PTR_LEN == 7);

	return mach_read_from_7(ptr);
}

/*通过space page_no获取page页对象,并获取page的x_latch*/
UNIV_INLINE page_t* trx_undo_page_get(ulint space, ulint page_no, mtr_t* mtr)
{
	page_t* page;
	page = buf_page_get(space, page_no, RW_X_LATCH, mtr);
	buf_page_dbg_add_level(page, SYNC_TRX_UNDO_PAGE);

	return page;
}

/*通过space page_no获取page页对象,并获取page的s_latch*/
UNIV_INLINE page_t* trx_undo_page_get_s_latched(ulint space, ulint page_no, mtr_t* mtr)
{
	page_t* page;
	page = buf_page_get(space, page_no, RW_S_LATCH, mtr);
	buf_page_dbg_add_level(page, SYNC_TRX_UNDO_PAGE);

	return page;
}

/*获得undo log rec在指定undo page的起始偏移位置*/
UNIV_INLINE ulint trx_undo_page_get_start(page_t* undo_page, ulint page_no, ulint offset)
{
	ulint start;

	if(page_no == buf_frame_get_page_no(undo_page))
		start = mach_read_from_2(offset + undo_page + TRX_UNDO_LOG_START);
	else
		start = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE;

	return start;
}
/*获得undo log rec在指定undo page的结束偏移位置*/
UNIV_INLINE ulint trx_undo_page_get_end(page_t* undo_page, ulint page_no, ulint offset)
{
	trx_ulogf_t*	log_hdr;
	ulint		end;

	if(page_no == buf_frame_get_page_no(undo_page)){
		log_hdr = undo_page + offset;
		end = mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG);
		if(end == 0) /*如果偏移为0,直接定位到undo page的可写空闲位置*/
			end = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
	}
	else
		end = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
}

/*获取rec前一条undo rec的句柄*/
UNIV_INLINE trx_undo_rec_t* trx_undo_page_get_prev_rec(trx_undo_rec_t* rec, ulint page_no, ulint offset)
{
	page_t*	undo_page;
	ulint	start;

	undo_page = buf_frame_align(rec);
	start = trx_undo_page_get_start(undo_page, page_no, offset);
	if(start + undo_page == rec) /*rec是第一条记录*/
		return NULL;

	return undo_page + mach_read_from_2(rec - 2);
}

/*获取rec后一条undo rec的句柄*/
UNIV_INLINE trx_undo_rec_t* trx_undo_page_get_next_rec(trx_undo_rec_t* rec, ulint page_no, ulint offset)
{
	page_t*	undo_page;
	ulint	end;
	ulint	next;

	undo_page = buf_frame_align(rec);
	end = trx_undo_page_get_end(undo_page, page_no, offset);
	next = mach_read_from_2(rec);
	if(next == end) /*rec已经是最后一条记录*/
		return NULL;

	return undo_page + next;
}

/*获取undo page中的最后一条记录*/
UNIV_INLINE trx_undo_rec_t* trx_undo_page_get_last_rec(page_t* undo_page, ulint page_no, ulint offset)
{
	ulint start, end;

	start = trx_undo_page_get_start(undo_page, page_no, offset);
	end = trx_undo_page_get_end(undo_page, page_no, offset);
	if(start == end)
		return NULL;

	return undo_page + mach_read_from_2(undo_page + end - 2);
}
/*获得undo page中的第一条记录*/
UNIV_INLINE trx_undo_rec_t* trx_undo_page_get_first_rec(page_t* undo_page, ulint page_no, ulint offset)
{
	ulint start, end;

	start = trx_undo_page_get_start(undo_page, page_no, offset);
	end = trx_undo_page_get_end(undo_page, page_no, offset);
	if(start == end)
		return NULL;

	return undo_page + start;
}

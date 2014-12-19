#include "mach0data.h"
#include "ut0lst.h"
#include "buf0buf.h"

/*打开一个mlog的buffer*/
UNIV_INLINE byte* mlog_open(mtr_t* mtr, ulint size)
{
	dyn_array_t* mlog;
	mtr->modifications = TRUE;

	if(mtr_get_log_mode(mtr) == MTR_LOG_NONE)
		return NULL;

	mlog = &(mtr->log);
	return dyn_array_open(mlog, size);
}

UNIV_INLINE void mlog_close(mtr_t* mtr, byte* ptr)
{
	dyn_array_t* mlog;
	ut_ad(mtr_get_log_mode(mtr) != MTR_LOG_NONE);

	mlog = &(mtr->log);

	dyn_array_close(mlog, ptr);
}

UNIV_INLINE void mlog_catenate_ulint(mtr_t* mtr, ulint val, ulint type)
{
	dyn_array_t*	mlog;
	byte*		ptr;

	if (mtr_get_log_mode(mtr) == MTR_LOG_NONE)
		return;
	
	mlog = &(mtr->log);

	ut_ad(MLOG_1BYTE == 1);
	ut_ad(MLOG_2BYTES == 2);
	ut_ad(MLOG_4BYTES == 4);

	ptr = dyn_array_push(mlog, type);

	if(type == MLOG_4BYTES)
		mach_write_to_4(ptr, val);
	else if(type == MLOG_2BYTES)
		mach_write_to_2(ptr, val);
	else{
		ut_ad(type == MLOG_1BYTE);
		mach_write_to_1(ptr, val);
	}
}

/*根据val的值大小确定写入的字节数*/
UNIV_INLINE void mlog_catenate_ulint_compressed(mtr_t* mtr, ulint val)
{
	byte* log_ptr;
	log_ptr = mlog_open(mtr, 10);
	if(log_ptr == NULL)
		return;

	log_ptr += mach_write_compressed(log_ptr, val);
	mlog_close(mtr, log_ptr);
}

UNIV_INLINE void mlog_catenate_dulint_compressed(mtr_t* mtr, dulint val)
{
	byte* log_ptr;
	log_ptr = mlog_open(mtr, 15);
	if(log_ptr == NULL)
		return;

	log_ptr += mach_dulint_write_compressed(log_ptr, val);
	mlog_close(mtr, log_ptr);
}

UNIV_INLINE byte* mlog_write_initial_log_record_fast(byte* ptr, byte type, byte* log_ptr, mtr_t* mtr)
{
	buf_block_t*	block;
	ulint			space;
	ulint			offset;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(ptr),  MTR_MEMO_PAGE_X_FIX));
	ut_ad(type <= MLOG_BIGGEST_TYPE);
	ut_ad(ptr && log_ptr);

	block = buf_block_align(ptr);

	space = buf_block_get_space(block);
	offset = buf_block_get_page_no(block); /*偏移是page no*/

	if(space != 0 || offset > 0x8FFFFFFF){ /*space和page_no不对*/
		fprintf(stderr,
			"InnoDB: error: buffer page pointer %lx has nonsensical space id %lu\n"
			"InnoDB: or page no %lu\n", (ulint)ptr, space, offset);
		ut_a(0);
	}

	mach_write_to_1(log_ptr, type);
	log_ptr++;	
	log_ptr += mach_write_compressed(log_ptr, space);
	log_ptr += mach_write_compressed(log_ptr, offset);

	mtr->n_log_recs ++;

#ifdef UNIV_DEBUG
	if(!mtr_memo_contains(mtr, block, MTR_MEMO_MODIFY))
		mtr_memo_push(mtr, block, MTR_MEMO_MODIFY);
#endif

	return log_ptr;
}


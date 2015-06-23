#include "mtr0log.h"
#include "buf0buf.h"
#include "dict0boot.h"
#include "log0recv.h"

void mlog_catenate_string(mtr_t* mtr, byte* str, ulint len)
{
	dyn_array_t* mlog;
	if(mtr_get_log_mode(mtr) == MTR_LOG_NONE)
		return;

	mlog = &(mlog->log);
	dyn_push_string(mlog, str, len);
}

void mlog_write_initial_log_record(byte* ptr, byte type, mtr_t* mtr)
{
	byte*	log_ptr;

	ut_ad(type <= MLOG_BIGGEST_TYPE);
	if(ptr < buf_pool->frame_zero || ptr >= buf_pool->high_end){
		fprintf(stderr,"InnoDB: Error: trying to write to a stray memory location %lx\n", (ulint)ptr);
		ut_a(0);
	}

	log_ptr = mlog_open(mtr, 20);
	if(log_ptr == NULL)
		return;

	log_ptr = mlog_write_initial_log_record_fast(ptr, type, log_ptr, mtr);
	mlog_close(mtr, log_ptr);
}

byte* mlog_parse_initial_log_record(byte* ptr, byte* end_ptr, byte* type, ulint* space, ulint* page_no)
{
	if(end_ptr < ptr + 1)
		return NULL;

	/*获得type*/
	*type = (byte)((ulint)*ptr & ~MLOG_SINGLE_REC_FLAG);
	ptr ++;

	if(end_ptr < ptr + 2)
		return NULL;
	/*获得space*/
	ptr = mach_parse_compressed(ptr, end_ptr, space);
	if(ptr == NULL)
		return NULL;
	/*获得page_no*/
	ptr = mach_parse_compressed(ptr, end_ptr, page_no);
	return ptr;
}

/*对mini transcation的整型数推演,没有特殊的操作关联，只是数据的修改*/
byte* mlog_parse_nbytes(ulint type, byte* ptr, byte* end_ptr, byte* page)
{
	ulint	offset;
	ulint	val;
	dulint	dval;

	if(end_ptr < ptr + 2)
		return NULL;

	offset = mach_read_from_2(ptr);
	ptr += 2;

	if(offset >= UNIV_PAGE_SIZE){
		recv_sys->found_corrupt_log = TRUE;
		return NULL;
	}

	if(type == MLOG_8BYTES){
		ptr = mach_dulint_parse_compressed(ptr, end_ptr, &dval);
		if(ptr == NULL)
			return NULL;

		if(page)
			mach_write_to_8(page + offset, dval);

		return ptr;
	}

	ptr = mach_parse_compressed(ptr, end_ptr, &val);
	if(ptr == NULL)
		return NULL;
	/*检查值的合法性*/
	if(type == MLOG_1BYTE){
		if (val > 0xFF) {
			recv_sys->found_corrupt_log = TRUE;
			return(NULL);
		}
	}
	else if(type == MLOG_2BYTES){
		if (val > 0xFFFF) {
			recv_sys->found_corrupt_log = TRUE;
			return(NULL);
		}
	}
	else{
		if (type != MLOG_4BYTES) {
			recv_sys->found_corrupt_log = TRUE;
			return(NULL);
		}
	}
	/*赋值操作*/
	if(page){
		if (type == MLOG_1BYTE)
			mach_write_to_1(page + offset, val);
		else if (type == MLOG_2BYTES)
			mach_write_to_2(page + offset, val);
		else{
			ut_a(type == MLOG_4BYTES);
			mach_write_to_4(page + offset, val);
		}
	}

	return ptr;
}

void mlog_write_ulint(byte* ptr, ulint val, byte type, mtr_t* mtr)
{
	byte*	log_ptr;

	if (ptr < buf_pool->frame_zero || ptr >= buf_pool->high_end) {
		fprintf(stderr, "InnoDB: Error: trying to write to a stray memory location %lx\n", (ulint)ptr);
		ut_a(0);
	}

	if (type == MLOG_1BYTE) {
		mach_write_to_1(ptr, val);
	} else if (type == MLOG_2BYTES) {
		mach_write_to_2(ptr, val);
	} else {
		ut_ad(type == MLOG_4BYTES);
		mach_write_to_4(ptr, val);
	}

	log_ptr = mlog_open(mtr, 30);
	if(log_ptr == NULL)
		return;

	log_ptr = mlog_write_initial_log_record_fast(ptr, type, log_ptr, mtr);
	mach_write_to_2(log_ptr, ptr - buf_frame_align(ptr));
	log_ptr += 2;

	log_ptr += mach_write_compressed(log_ptr, val);

	mlog_close(mtr, log_ptr);
}

void mlog_write_dulint(byte* ptr, dulint val, byte type, mtr_t* mtr)
{
	byte*	log_ptr;

	if (ptr < buf_pool->frame_zero || ptr >= buf_pool->high_end) {
		fprintf(stderr, "InnoDB: Error: trying to write to a stray memory location %lx\n", (ulint)ptr);
		ut_a(0);
	}

	ut_ad(ptr && mtr);
	ut_ad(type == MLOG_8BYTES);

	mach_write_to_8(ptr, val);

	log_ptr = mlog_open(mtr, 30);
	if(log_ptr == NULL)
		return;

	log_ptr = mlog_write_initial_log_record_fast(ptr, type, log_ptr, mtr);
	mach_write_to_2(log_ptr, ptr - buf_frame_align(ptr));
	log_ptr += 2;

	log_ptr += mach_dulint_write_compressed(log_ptr, val);
	
	mlog_close(mtr, log_ptr);
}

void mlog_write_string(byte* ptr, byte* str, ulint len, mtr_t* mtr)
{
	byte* log_ptr;

	if (ptr < buf_pool->frame_zero || ptr >= buf_pool->high_end) {
		fprintf(stderr, "InnoDB: Error: trying to write to a stray memory location %lx\n", (ulint)ptr);
		ut_a(0);
	}

	ut_ad(ptr && mtr);
	ut_a(len < UNIV_PAGE_SIZE);

	ut_memcpy(ptr, str, len);
	log_ptr = mlog_open(mtr, 30);
	if(log_ptr == NULL)
		return;

	log_ptr = mlog_write_initial_log_record_fast(ptr, MLOG_WRITE_STRING, log_ptr, mtr);
	mach_write_to_2(log_ptr, ptr - buf_frame_align(ptr));
	log_ptr += 2;

	mach_write_to_2(log_ptr, len);
	log_ptr += 2;

	mlog_catenate_string(mtr, str, len);
}

byte* mlog_parse_string(byte* ptr, byte* end_ptr, byte* page)
{
	ulint	offset;
	ulint	len;

	if(end_ptr < ptr + 4)
		return NULL;

	offset = mach_read_from_2(ptr);
	ptr += 2;

	if(offset >= UNIV_PAGE_SIZE){
		recv_sys->found_corrupt_log = TRUE;
		return(NULL);
	}

	len = mach_read_from_2(ptr);
	ptr += 2;
	if(end_ptr < ptr + len)
		return NULL;

	if(page)
		ut_memcpy(page + offset, ptr, len);

	return ptr + len;
}

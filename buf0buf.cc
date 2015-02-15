#include "buf0buf.h"
#include "mem0mem.h"
#include "btr0btr.h"
#include "fil0fil.h"
#include "lock0lock.h"
#include "btr0sea.h"
#include "ibuf0ibuf.h"
#include "dict0dict.h"
#include "log0recv.h"
#include "trx0undo.h"
#include "srv0srv.h"

buf_pool_t* buf_pool		= NULL;
ulint buf_dbg_counter		= 0;
ibool buf_debug_prints		= FALSE;

/*计算一个page内容的hash值*/
ulint buf_calc_page_checksum(byte* page)
{
	ulint checksum;

	checksum = ut_fold_binary(page, FIL_PAGE_FILE_FLUSH_LSN) + ut_fold_binary(page + FIL_PAGE_DATA, UNIV_PAGE_SIZE - FIL_PAGE_DATA - FIL_PAGE_END_LSN);
	checksum = checksum & 0xFFFFFFFF;

	return checksum;
}

/*判断page是否损坏了,一般是从磁盘将page导入缓冲池时要做判断*/
ibool buf_page_is_corrupted(byte* read_buf)
{
	ulint checksum;

	checksum = buf_calc_page_checksum(read_buf);
	/*校验page的LSN和checksum*/
	if((mach_read_from_4(read_buf + FIL_PAGE_LSN + 4) != mach_read_from_4(read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN + 4))
		|| (checksum != mach_read_from_4(read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN)
		&& mach_read_from_4(read_buf + FIL_PAGE_LSN) != mach_read_from_4(read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN)))
		return TRUE;

	return FALSE;
}

/*打印page的错误信息,一般是在buf_page_is_corrupted判断后做错误输出*/
void buf_page_print(byte* read_buf)
{
	dict_index_t*	index;
	ulint		checksum;
	char*		buf;

	buf = mem_alloc(4 * UNIV_PAGE_SIZE);

	ut_sprintf_buf(buf, read_buf, UNIV_PAGE_SIZE);

	ut_print_timestamp(stderr);
	fprintf(stderr, "  InnoDB: Page dump in ascii and hex (%lu bytes):\n%s", (ulint)UNIV_PAGE_SIZE, buf);
	fprintf(stderr, "InnoDB: End of page dump\n");

	mem_free(buf);

	checksum = buf_calc_page_checksum(read_buf);

	ut_print_timestamp(stderr);
	fprintf(stderr, "  InnoDB: Page checksum %lu stored checksum %lu\n",
		checksum, mach_read_from_4(read_buf+ UNIV_PAGE_SIZE - FIL_PAGE_END_LSN)); 

	fprintf(stderr, "InnoDB: Page lsn %lu %lu, low 4 bytes of lsn at page end %lu\n",
		mach_read_from_4(read_buf + FIL_PAGE_LSN),
		mach_read_from_4(read_buf + FIL_PAGE_LSN + 4),
		mach_read_from_4(read_buf + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN + 4));

	if (mach_read_from_2(read_buf + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_INSERT)
		fprintf(stderr, "InnoDB: Page may be an insert undo log page\n");
	else if(mach_read_from_2(read_buf + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_UPDATE)
		fprintf(stderr, "InnoDB: Page may be an update undo log page\n");

	if(fil_page_get_type(read_buf) == FIL_PAGE_INDEX) {
		fprintf(stderr, "InnoDB: Page may be an index page ");

		fprintf(stderr, "where index id is %lu %lu\n",
			ut_dulint_get_high(btr_page_get_index_id(read_buf)),
			ut_dulint_get_low(btr_page_get_index_id(read_buf)));

		index = dict_index_find_on_id_low(btr_page_get_index_id(read_buf));
		if (index)
			fprintf(stderr, "InnoDB: and table %s index %s\n", index->table_name, index->name);
	}
}

static void buf_block_init(buf_block_t* block, byte* frame)
{
	/*初始化状态信息*/
	block->state = BUF_BLOCK_NOT_USED;
	block->frame = frame;
	block->modify_clock = ut_dulint_zero;
	block->file_page_was_freed = FALSE;

	/*初始化latch*/
	rw_lock_create(&(block->lock));
	ut_ad(rw_lock_validate(&(block->lock)));

	rw_lock_create(&(block->read_lock));
	rw_lock_set_level(&(block->read_lock), SYNC_NO_ORDER_CHECK);

	rw_lock_create(&(block->debug_latch));
	rw_lock_set_level(&(block->debug_latch), SYNC_NO_ORDER_CHECK);
}

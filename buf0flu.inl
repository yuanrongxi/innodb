#include "buf0buf.h"
#include "mtr0mtr.h"


void buf_flush_insert_into_flush_list(buf_block_t* block);

void buf_flush_insert_sorted_into_flush_list(buf_block_t* block);

/*脏页刷盘前调用，设置脏页的start_lsn和end_lsn*/
UNIV_INLINE void buf_flush_note_modification(buf_block_t* block, mtr_t* mtr)
{
	ut_ad(block);
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);
	ut_ad(block->buf_fix_count > 0);
	ut_ad(rw_lock_own(&(block->lock), RW_LOCK_EX));
	ut_ad(mutex_own(&(buf_pool->mutex)));

	ut_ad(ut_dulint_cmp(mtr->start_lsn, ut_dulint_zero) != 0);
	ut_ad(mtr->modifications);
	ut_ad(ut_dulint_cmp(block->newest_modification, mtr->end_lsn) <= 0);

	/*将MINI TRANSCATION最后的LSN保存到对应的block当中,符合Write-Ahead Log原则*/
	block->newest_modification = mtr->end_lsn;
	/*假如oldest lsn = 0,将mtr->start_lsn赋值给oldest lsn*/
	if(ut_dulint_is_zero(block->oldest_modification)){
		block->oldest_modification = mtr->start_lsn;
		ut_ad(!ut_dulint_is_zero(block->old));

		buf_flush_insert_into_flush_list(block);
	}
	else
		ut_ad(ut_dulint_cmp(block->oldest_modification, mtr->start_lsn) <= 0);
}

/*redo log重演时，设置脏页的start_lsn和end_lsn*/
UNIV_INLINE void buf_flush_recv_note_modification(buf_block_t* block, dulint start_lsn, dulint end_lsn)
{
	ut_ad(block);
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);
	ut_ad(block->buf_fix_count > 0);
	ut_ad(rw_lock_own(&(block->lock), RW_LOCK_EX));

	mutex_enter(&(buf_pool->mutex));

	ut_ad(ut_dulint_cmp(block->newest_modification, end_lsn) <= 0);

	block->newest_modification = end_lsn;
	
	if(ut_dulint_is_zero(block->oldest_modification)){
		block->oldest_modification = start_lsn;
		ut_ad(!ut_dulint_is_zero(block->oldest_modification));

		buf_flush_insert_sorted_into_flush_list(block);
	}
	else
		ut_ad(ut_dulint_cmp(block->oldest_modification, start_lsn) <= 0);

	mutex_exit(&(buf_pool->mutex));
}








#include "mtr0mtr.h"
#include "buf0buf.h"
#include "page0types.h"
#include "mtr0log.h"
#include "log0log.h"

mtr_t* mtr_start_noninline(mtr_t* mtr)
{
	return mtr_start(mtr);
}

/*释放slot的控制权*/
UNIV_INLINE void mtr_memo_slot_release(mtr_t* mtr, mtr_memo_slot_t* slot)
{
	void* object;
	ulint type;

	object = slot->object;
	type = slot->type;
	if(object != NULL){
		if(type <= MTR_MEMO_BUF_FIX)
			buf_page_release((buf_block_t*)object, type, mtr);
		else if(type == MTR_MEMO_S_LOCK)
			rw_lock_s_unlock((rw_lock_t*)object);
#ifndef UNIV_DEBUG
		else
			rw_lock_x_unlock((rw_lock_t*)object);
#endif
#ifdef UNIV_DEBUG
		else if (type == MTR_MEMO_X_LOCK)
			rw_lock_x_unlock((rw_lock_t*)object);
		else{
			ut_ad(type == MTR_MEMO_MODIFY);
			ut_ad(mtr_memo_contains(mtr, object, MTR_MEMO_PAGE_X_FIX));
		}
#endif
	}

	slot->object = NULL;
}

/*释放掉所有memo中的控制权*/
UNIV_INLINE void mtr_memo_pop_all(mtr_t* mtr)
{
	mtr_memo_slot_t*	slot;
	dyn_array_t*		memo;
	ulint				offset;

	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);
	ut_ad(mtr->state == MTR_COMMITTING); 

	memo = &(mtr->memo);
	offset = dyn_array_get_data_size(memo);
	while(offset > 0){
		offset -= sizeof(mtr_memo_slot_t);
		slot = dyn_array_get_element(memo, offset);
		mtr_memo_slot_release(mtr, slot);
	}
}

/*将page的修改操作刷到log中*/
static void mtr_log_write_full_page(page_t* page, ulint i, ulint n_pages, mtr_t* mtr)
{
	byte*	buf;
	byte*	ptr;
	ulint	len;

	buf = mem_alloc(UNIV_PAGE_SIZE + 50);
	/*将space和page_no写入到buf当红中*/
	ptr = mlog_write_initial_log_record_fast(page, MLOG_FULL_PAGE, buf, mtr);
	ut_memcpy(ptr, page, UNIV_PAGE_SIZE); /*将整个page的内容作为日志放入缓冲区中*/

	len = (ptr - buf) + UNIV_PAGE_SIZE;
	
	if(i == n_pages - 1){
		if(n_pages > 1){ /*多页操作作为一个log rec写入日志系统*/
			*(buf + len) = MLOG_MULTI_REC_END;
			len ++;
		}
		else
			*buf = (byte)((ulint)*buf | MLOG_SINGLE_REC_FLAG);
	}

	ut_ad(len < UNIV_PAGE_SIZE + 50);
	/*将mtr的日志写入到redo log系统中*/
	log_write_low(buf, len);

	mem_free(buf);
}

byte* mtr_log_parse_full_page(byte* ptr, byte* end_ptr, page_t* page)
{
	if(end_ptr < ptr + UNIV_PAGE_SIZE) /*ptr没有一整个页的内容*/
		return NULL;

	if(page)
		ut_memcpy(page, ptr, UNIV_PAGE_SIZE);

	return prt + UNIV_PAGE_SIZE;
}

void mtr_log_write_backup_full_pages(mtr_t* mtr, ulint n_pages)
{
	mtr_memo_slot_t* slot;
	dyn_array_t* memo;
	buf_block_t* block;
	ulint		offset;
	ulint		type;
	ulint		i;

	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);
	ut_ad(mtr->state == MTR_COMMITTING);

	/*为log_write_low做准备，会判断redo log buffer的是否要刷盘*/
	mtr->start_lsn = log_reserve_and_open(n_pages * (UNIV_PAGE_SIZE + 50));
	memo =&(mtr->memo);
	offset = dyn_array_get_data_size(memo);

	i = 0;
	/*将mtr中所有MTR_MEMO_PAGE_X_FIX的slot对应的block pages刷到日志里面*/
	while(offset > 0){
		offset -= sizeof(mtr_memo_slot_t);
		slot = dyn_array_get_element(memo, offset);

		block = slot->object;
		type = slot->type;

		if(block != NULL && type == MTR_MEMO_PAGE_X_FIX){
			mtr_log_write_full_page(block->frame, i, n_pages, mtr);
			i ++;
		}
	}

	ut_ad(i == n_pages);
}

/*判断在线备份是在mtr首次更改页的操作是不之后*/
static ibool mtr_first_to_modify_page_after_backup(mtr_t* mtr, ulint* n_pages)
{
	mtr_memo_slot_t* slot;
	dyn_array_t*	memo;
	ulint		offset;
	buf_block_t*	block;
	ulint		type;
	dulint		backup_lsn;
	ibool		ret	= FALSE;

	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);
	ut_ad(mtr->state == MTR_COMMITTING);

	backup_lsn = log_get_online_backup_lsn_low();

	memo = &mtr->memo;
	offset = dyn_array_get_data_size(memo);
	*n_pages = 0;

	/*O(N * N)的复杂度，是否可以优化？*/
	while(offset > 0){
		offset -= sizeof(mtr_memo_slot_t);
		slot = dyn_array_get_element(memo, offset);

		block = slot->object;
		type = slot->type;

		if(block != NULL && type == MTR_MEMO_PAGE_X_FIX){
			*n_pages = *n_pages + 1;
			if(ut_dulint_cmp(buf_frame_get_newest_modification(block->frame), backup_lsn) <= 0){ /*mtr中的page newest lsn小于backup_lsn,说明有page只在backup之前做了更改操作*/
				printf("Page %lu newest %lu backup %lu\n",
					block->offset,
					ut_dulint_get_low(buf_frame_get_newest_modification(block->frame)),
					ut_dulint_get_low(backup_lsn));

				ret = TRUE;
			}
		}
	}

	return ret;
}

static void mtr_log_reserve_and_write(mtr_t* mtr)
{
	dyn_array_t*	mlog;
	dyn_block_t*	block;
	ulint		data_size;
	ibool		success;
	byte*		first_data;
	ulint		n_modified_pages;

	ut_ad(mtr);

	mlog = &mtr->log;
	first_data = dyn_block_get_data(mlog);

	if(mtr->n_log_recs > 1)
		mlog_catenate_ulint(mtr, MLOG_MULTI_REC_END, MLOG_1BYTE); /*写入一个操作type*/
	else
		*first_data = (byte)((ulint)*first_data | MLOG_SINGLE_REC_FLAG);

	if(mlog->heap == NULL){
		/*将mlog中的日志信息快速刷入log_sys->buf当中*/
		mtr->end_lsn = log_reserve_and_write_fast(first_data, dyn_block_get_used(mlog), &(mtr->start_lsn), &success);
		if(success)
			return ;
	}

	/*获得mlog中log数据的总长度*/
	data_size =dyn_array_get_data_size(mlog);
	mtr->start_lsn = log_reserve_and_open(data_size);
	if(mtr->log_mode == MTR_LOG_ALL){
		/*数据正在热备且备份之前有页改动*/
		if(log_get_online_backup_state_low() && mtr_first_to_modify_page_after_backup(mtr, &n_modified_pages)){
			log_close();
			log_release();

			/*将memo中的操作数据全部刷日志中*/
			mtr_log_write_backup_full_pages(mtr, n_modified_pages);
		}
		else{
			block = mlog;
			/*将mlog中的操作日志数据逐步刷入redo log buffer当中*/
			while(block != NULL){
				log_write_low(dyn_block_get_data(block),dyn_block_get_used(block));
				block = dyn_array_get_next_block(mlog, block);
			}
		}
	}
	else{
		ut_ad(mtr->log_mode == MTR_LOG_NONE);
	}

	/*更新最后的log lsn,就是log_sys->lsn*/
	mtr->end_lsn = log_close();
}

/*提交mtr*/
void mtr_commit(mtr_t* mtr)
{
	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);
	ut_ad(mtr->state == MTR_ACTIVE);
#ifdef UNIV_DEBUG
	mtr->state = MTR_COMMITTING;
#endif

	if (mtr->modifications)
		mtr_log_reserve_and_write(mtr);

	/*释放掉mtr中所有latch的控制权*/
	mtr_memo_pop_all(mtr);

	if (mtr->modifications)
		log_release();

#ifdef UNIV_DEBUG
	mtr->state = MTR_COMMITTED;
#endif

	dyn_array_free(&(mtr->memo));
	dyn_array_free(&(mtr->log));
}

/*mtr回滚到savepoint点*/
void mtr_rollback_to_savepoint(mtr_t* mtr, ulint savepoint)
{
	mtr_memo_slot_t*	slot;
	dyn_array_t*		memo;
	ulint				offset;

	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);
	ut_ad(mtr->state == MTR_ACTIVE);

	memo = &(mtr->memo);
	offset = dyn_array_get_data_size(memo);
	ut_ad(offset >= savepoint);
	while(offset > savepoint){
		offset -= sizeof(mtr_memo_slot_t);

		slot = dyn_array_get_element(memo, offset);

		ut_ad(slot->type != MTR_MEMO_MODIFY);
		/*释放控制锁*/
		mtr_memo_slot_release(mtr, slot);
	}
}

/*释放object type对应的slot控制权*/
void mtr_memo_release(mtr_t* mtr, void* object, ulint type)
{
	mtr_memo_slot_t*	slot;
	dyn_array_t*		memo;
	ulint				offset;

	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);
	ut_ad(mtr->state == MTR_ACTIVE);

	memo = &(mtr->memo);
	offset = dyn_array_get_data_size(memo);
	while(offset > 0){
		offset -= sizeof(mtr_memo_slot_t);
		
		slot = dyn_array_get_element(memo, offset);
		if((object == slot->object) && (type == slot->type)){
			mtr_memo_slot_release(mtr, slot);
			break;
		}
	}
}

ulint mtr_read_ulint(byte* ptr, ulint type, mtr_t* mtr)
{
	ut_ad(mtr->state == MTR_ACTIVE);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(ptr), MTR_MEMO_PAGE_S_FIX) || mtr_memo_contains(mtr, buf_block_align(ptr), MTR_MEMO_PAGE_X_FIX));
	
	if(type == MLOG_1BYTE)
		return mach_read_from_1(ptr);
	else if(type == MLOG_2BYTES)
		return(mach_read_from_2(ptr));
	else{
		ut_ad(type == MLOG_4BYTES);
		return(mach_read_from_4(ptr));
	}
}

dulint mtr_read_dulint(byte* ptr, ulint type, mtr_t* mtr)
{
	ut_ad(mtr->state == MTR_ACTIVE);
	ut_ad(ptr && mtr);
	ut_ad(type == MLOG_8BYTES);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(ptr), MTR_MEMO_PAGE_S_FIX) || mtr_memo_contains(mtr, buf_block_align(ptr), MTR_MEMO_PAGE_X_FIX));

	return mach_read_from_8(ptr);
}

/*答应mtr的信息*/
void mtr_print(mtr_t* mtr)
{
	printf("Mini-transaction handle: memo size %lu bytes log size %lu bytes\n", 
		dyn_array_get_data_size(&(mtr->memo)), dyn_array_get_data_size(&(mtr->log)));
}




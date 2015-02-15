#include "buf0flu.h"
#include "buf0lru.h"
#include "buf0rea.h"
#include "mtr0mtr.h"

extern ulint buf_dbg_counter;

/*判断block是否可以放入younger list当中*/
UNIV_INLINE ibool buf_block_peek_if_too_old(buf_block_t* block)
{
	if(buf_pool->freed_page_clock >= block->freed_page_clock + 1 + (buf_pool->curr_size / 1024))
		return TRUE;

	return FALSE;
}

/*获得当前缓冲池使用空间大小*/
UNIV_INLINE ulint buf_pool_get_curr_size()
{
	return buf_pool->curr_size * UNIV_PAGE_SIZE;
}

/*获得缓冲池最大空间大小，在mysql的配置文件中为buffer_pool_size的配置*/
UNIV_INLINE buf_block_t* buf_pool_get_max_size()
{
	return buf_pool->max_size * UNIV_PAGE_SIZE;
}

/*获得buffer pool中的第i个block*/
UNIV_INLINE buf_block_t* buf_pool_get_nth_block(buf_pool_t* pool, ulint i)
{
	ut_ad(buf_pool);
	ut_ad(i < buf_pool->max_size);

	return i + buf_pool->blocks;
}

/*检查ptr是否是buffer pool blocks中的指针*/
UNIV_INLINE ibool buf_pool_is_block(void* ptr)
{
	if(buf_pool->blocks <= (buf_block_t*)ptr && (buf_block_t*)ptr < buf_pool->blocks + buf_pool->max_size)
		return TRUE;

	return FALSE;
}

/*获得lru队列中最早修改的block的对应lsn*/
UNIV_INLINE dulint buf_pool_get_oldest_modification(void)
{
	buf_block_t*	block;
	dulint			lsn;

	mutex_enter(&(buf_pool->mutex));

	block = UT_LIST_GET_LAST(buf_pool->flush_list);
	if(block == NULL)
		lsn = ut_dulint_zero;
	else
		lsn = block->oldest_modification;

	mutex_exit(&(buf_pool->mutex));

	return lsn;
}

/*pool clock 自加1*/
UNIV_INLINE ulint buf_pool_clock_tic()
{
	ut_ad(mutex_own(&(buf_pool->mutex)));

	buf_pool->ulint_clock ++;

	return buf_pool->ulint_clock;
}

/*获得block对应的frame指针*/
UNIV_INLINE buf_frame_t* buf_block_get_frame(buf_block_t* block)
{
	ut_ad(block);
	ut_ad(block >= buf_pool->blocks);
	ut_ad(block < buf_pool->blocks + buf_pool->max_size);
	ut_ad(block->state != BUF_BLOCK_NOT_USED); 
	ut_ad((block->state != BUF_BLOCK_FILE_PAGE) || (block->buf_fix_count > 0));

	return block->frame;
}

/*获得block对应的space id*/
UNIV_INLINE ulint buf_block_get_space(buf_block_t* block)
{
	ut_ad(block);
	ut_ad(block >= buf_pool->blocks);
	ut_ad(block < buf_pool->blocks + buf_pool->max_size);
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);
	ut_ad(block->buf_fix_count > 0);

	return block->space;
}

/*获得block对应的page no*/
UNIV_INLINE ulint buf_block_get_page_no(buf_block_t* block)
{
	ut_ad(block);
	ut_ad(block >= buf_pool->blocks);
	ut_ad(block < buf_pool->blocks + buf_pool->max_size);
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);
	ut_ad(block->buf_fix_count > 0);

	return block->offset;
}

/*查找ptr所在的block指针地址*/
UNIV_INLINE buf_block_t* buf_block_align(byte* ptr)
{
	buf_block_t* block;
	buf_frame_t* frame_zero;

	ut_ad(ptr);

	frame_zero = buf_pool->frame_zero;
	ut_ad((ulint)ptr >= (ulint)frame_zero);

	block = buf_pool_get_nth_block(buf_pool, ((ulint)(ptr - frame_zero)) >> UNIV_PAGE_SIZE_SHIFT);
	/*block不在buf pool的blocks地址范围中，是异常情况*/
	if(block < buf_pool->blocks || block >= buf_pool->blocks + buf_pool->max_size){
		fprintf(stderr,
			"InnoDB: Error: trying to access a stray pointer %lx\n"
			"InnoDB: buf pool start is at %lx, number of pages %lu\n", (ulint)ptr,
			(ulint)frame_zero, buf_pool->max_size);

		ut_a(0);
	}

	return block;
}

/*与buf_block_align功能相同*/
UNIV_INLINE buf_block_t* buf_block_align_low(byte* ptr)
{
	buf_block_t*	block;
	buf_frame_t*	frame_zero;

	ut_ad(ptr);

	frame_zero = buf_pool->frame_zero;

	ut_ad((ulint)ptr >= (ulint)frame_zero);

	block = buf_pool_get_nth_block(buf_pool, ((ulint)(ptr - frame_zero)) >> UNIV_PAGE_SIZE_SHIFT);
	if (block < buf_pool->blocks || block >= buf_pool->blocks + buf_pool->max_size) {

			fprintf(stderr,
				"InnoDB: Error: trying to access a stray pointer %lx\n"
				"InnoDB: buf pool start is at %lx, number of pages %lu\n", (ulint)ptr,
				(ulint)frame_zero, buf_pool->max_size);
			ut_a(0);
	}

	return block;
}

/*获得ptr所在的page指针地址*/
UNIV_INLINE buf_frame_t* buf_frame_align(byte* ptr)
{
	buf_frame_t* frame;

	ut_ad(ptr);

	frame = ut_align_down(ptr, UNIV_PAGE_SIZE);

	if ((void *)frame < (void*)(buf_pool->frame_zero)
		|| (void *)frame > (void *)(buf_pool_get_nth_block(buf_pool, buf_pool->max_size - 1)->frame)){
			fprintf(stderr,
				"InnoDB: Error: trying to access a stray pointer %lx\n"
				"InnoDB: buf pool start is at %lx, number of pages %lu\n", (ulint)ptr,
				(ulint)(buf_pool->frame_zero), buf_pool->max_size);
			ut_a(0);
	}

	return frame;
}

/*获得ptr所在block的page no,也就是通过page指针获得对应page的block信息*/
UNIV_INLINE ulint buf_frame_get_page_no(byte* ptr)
{
	return buf_block_get_page_no(buf_block_align(ptr));
}

/*获得ptr所在block的space id*/
UNIV_INLINE ulint buf_frame_get_space_id(byte* ptr)
{
	return buf_block_get_space(buf_block_align(ptr));
}

/*通过ptr获取一个磁盘链表的位置信息(fil_addr_t)*/
UNIV_INLINE void buf_ptr_get_fsp_addr(byte* ptr, ulint* space, fil_addr_t* addr)
{
	buf_block_t* block;

	block = buf_block_align(ptr);

	*space = buf_block_get_space(block);
	addr->page = buf_block_get_page_no(block);
	addr->boffset = ptr - buf_frame_align(ptr);
}

UNIV_INLINE ulint buf_frame_get_lock_hash_val(byte* ptr)
{
	buf_block_t* block;
	block = buf_block_align(ptr);

	return block->lock_hash_val;
}

UNIV_INLINE mutex_t* buf_frame_get_lock_mutex(byte* ptr)
{
	buf_block_t* block;
	block = buf_block_align(ptr);

	return block->lock_mutex;
}
/*将frame的内容拷贝到buf中*/
UNIV_INLINE byte* buf_frame_copy(byte* buf, buf_frame_t* frame)
{
	ut_ad(buf && frame);

	ut_memcpy(buf, frame, UNIV_PAGE_SIZE);
}

/*用space和page no计算一个page的fold信息*/
UNIV_INLINE ulint buf_page_address_fold(ulint space, ulint offset)
{
	return((space << 20) + space + offset);
}

/*判断一个io操作是否正在作用于block对应的page*/
UNIV_INLINE ibool buf_page_io_query(buf_block_t* block)
{
	mutex_enter(&(buf_pool->mutex));

	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);
	ut_ad(block->buf_fix_count > 0);

	if(block->io_fix != 0){
		mutex_exit(&(buf_pool->mutex));
		return TRUE;
	}

	mutex_exit(&(buf_pool->mutex));

	return FALSE;
}

/*获得frame对应的block的newest modification（LSN）*/
UNIV_INLINE dulint buf_frame_get_newest_modification(buf_frame_t* frame)
{
	buf_block_t*	block;
	dulint			lsn;

	ut_ad(frame);

	block = buf_block_align(frame);

	mutex_enter(&(buf_pool->mutex));

	if (block->state == BUF_BLOCK_FILE_PAGE)
		lsn = block->newest_modification;
	else 
		lsn = ut_dulint_zero;

	mutex_exit(&(buf_pool->mutex));

	return lsn;
}

/*对frame对应的block的modify_clock做自加*/
UNIV_INLINE dulint buf_frame_modify_clock_inc(buf_frame_t* frame)
{
	buf_block_t*	block;

	ut_ad(frame);

	block = buf_block_align_low(frame);
	ut_ad((mutex_own(&(buf_pool->mutex)) && (block->buf_fix_count == 0)) || rw_lock_own(&(block->lock), RW_LOCK_EXCLUSIVE));

	UT_DULINT_INC(block->modify_clock);

	return block->modify_clock;
}

/*获得frame对应block的modify_clock*/
UNIV_INLINE dulint buf_frame_get_modify_clock(buf_frame_t*	frame)
{
	buf_block_t*	block;

	ut_ad(frame);

	block = buf_block_align(frame);

	ut_ad(rw_lock_own(&(block->lock), RW_LOCK_SHARED)
		|| rw_lock_own(&(block->lock), RW_LOCK_EXCLUSIVE));

	return block->modify_clock;
}

UNIV_INLINE void buf_block_buf_fix_inc_debug(buf_block_t* block, char* file, ulint line)
{
	ibool	ret;
	ret = rw_lock_s_lock_func_nowait(&(block->debug_latch), file, line);
	ut_ad(ret);

	block->buf_fix_count ++;
}

UNIV_INLINE void buf_block_buf_fix_inc(buf_block_t* block)
{
	block->buf_fix_count ++;
}

/*根据space id和page no在buf pool查找对应的block,如果没有被缓冲池缓冲的话，返回为NULL*/
UNIV_INLINE buf_block_t* buf_page_hash_get(ulint space, ulint offset)
{
	buf_block_t*	block;
	ulint			fold;

	ut_ad(buf_pool);
	ut_ad(mutex_own(&(buf_pool->mutex)));

	fold = buf_page_address_fold(space, offset);

	/*根据fold在buf pool的page hash中查找对应的block*/
	HASH_SEARCH(hash, buf_pool->page_hash, fold, block, (block->space == space) && (block->offset == offset));

	return block;
}

/*尝试获得一个page,如果page不在缓冲池中，就会触发file io从磁盘导入到缓冲池中，这时需要将所page所持有的mtr latch全部release*/
UNIV_INLINE buf_frame_t* buf_page_get_release_on_io(ulint space, ulint offset, buf_frame_t* guess, ulint rw_latch, ulint savepoint, mtr_t* mtr)
{
	buf_frame_t*	frame;

	frame = buf_page_get_gen(space, offset, rw_latch, guess, BUF_GET_IF_IN_POOL, __FILE__, __LINE__, mtr);
	if(frame != NULL)
		return frame;

	mtr_rollback_to_savepoint(mtr, savepoint);
	buf_page_get(space, offset, RW_S_LATCH, mtr);
	mtr_rollback_to_savepoint(mtr, savepoint);

	return NULL;
}

/*对block的buf_fix_count进行自减，并且release 指定的rw_latch的block->lock*/
UNIV_INLINE void buf_page_release(buf_block_t* block, ulint rw_latch, mtr_t* mtr)
{
	ulint	buf_fix_count;

	ut_ad(block);

	mutex_enter_fast(&(buf_pool->mutex));

	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);
	ut_ad(block->buf_fix_count > 0);

	if (rw_latch == RW_X_LATCH && mtr->modifications) 
		buf_flush_note_modification(block, mtr);

#ifdef UNIV_SYNC_DEBUG
	rw_lock_s_unlock(&(block->debug_latch));
#endif
	/*为什么怎么写？难道是为了并发？*/
	buf_fix_count = block->buf_fix_count;
	block->buf_fix_count = buf_fix_count - 1;

	mutex_exit(&(buf_pool->mutex));

	/*释放指定类型的rw_lock*/
	if (rw_latch == RW_S_LATCH)
		rw_lock_s_unlock(&(block->lock));
	else if (rw_latch == RW_X_LATCH)
		rw_lock_x_unlock(&(block->lock));
}

void buf_page_dbg_add_level(buf_frame_t* frame, ulint level)
{
#ifdef UNIV_SYNC_DEBUG
	sync_thread_add_level(&(buf_block_align(frame)->lock), level);
#endif
}

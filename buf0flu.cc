#include "buf0flu.h"

#include "ut0byte.h"
#include "ut0lst.h"
#include "fil0fil.h"
#include "buf0buf.h"
#include "buf0lru.h"
#include "buf0rea.h"
#include "ibuf0ibuf.h"
#include "log0log.h"
#include "os0file.h"
#include "trx0sys.h"

/*flush刷盘的页数*/
#define BUF_FLUSH_AREA		ut_min(BUF_READ_AHEAD_AREA, buf_pool->curr_size / 16)

/*判断flush list的合法性*/
static ibool buf_flush_validate_low();

/*将一个脏页对应的block插入到flush list当中*/
void buf_flush_insert_into_flush_list(buf_block_t* block)
{
	ut_ad(mutex_own(&(buf_pool->mutex)));

	ut_ad((UT_LIST_GET_FIRST(buf_pool->flush_list) == NULL)
		|| (ut_dulint_cmp((UT_LIST_GET_FIRST(buf_pool->flush_list))->oldest_modification, block->oldest_modification) <= 0));

	UT_LIST_ADD_FIRST(flush_list, buf_pool->flush_list, block);

	ut_ad(buf_flush_validate_low());
}

/*按start_lsn的由大到小的顺序将block插入到flush list当中，只有在redo log重演的过程才会调用此函数*/
void buf_flush_insert_sorted_into_flush_list(buf_block_t* block)
{
	buf_block_t*	prev_b;
	buf_block_t*	b;

	ut_ad(mutex_own(&(buf_pool->mutex)));

	prev_b = NULL;
	b = UT_LIST_GET_FIRST(buf_pool->flush_list);
	/*找到按LSN由大到小排序的位置，因为刷盘是按照LSN由小到大刷盘，从flush list的末尾开始刷入*/
	while(b && ut_ulint_cmp(b->oldest_modification, block->oldest_modification) > 0){
		prev_b = b;
		b = UT_LIST_GET_NEXT(flush_list, b);
	}

	/*prev_b == NULL,说明block->start_lsn比队列中任何block都要大，所以插入到flush list的头上*/
	if(prev_b == NULL)
		UT_LIST_ADD_FIRST(flush_list, buf_pool->flush_list, block);
	else
		UT_LIST_INSERT_AFTER(flush_list, buf_pool->flush_list, prev_b, block);

	ut_ad(buf_flush_validate_low());
}

/*检查block是否可以进行置换淘汰，如果有IO操作或者fix latch存在、已经被修改过还没有刷入磁盘，不能进行置换*/
ibool buf_flush_ready_for_replace(buf_block_t* block)
{
	ut_ad(mutex_own(&(buf_pool->mutex)));
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);

	if((ut_dulint_cmp(block->oldest_modification, ut_dulint_zero) > 0) 
		|| block->buf_fix_count != 0 || block->io_fix != 0)
		return FALSE;
	
	return TRUE;
}

/*检查block对应的page是脏页，并且可以进行flush到磁盘上*/
UNIV_INLINE ibool buf_flush_ready_for_flush(buf_block_t* block, ulint flush_type)
{
	ut_ad(mutex_own(&(buf_pool->mutex)));
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);

	if(ut_dulint_cmp(block->oldest_modification, ut_dulint_zero) > 0 && block->io_fix == 0){
		if(flush_type != BUF_FLUSH_LRU)
			return TRUE;
		else if(block->buf_fix_count == 0)
			return TRUE;
	}

	return FALSE;
}

/*当一个flush完成信号到达时，修改对应的状态信息*/
void buf_flush_write_complete(buf_block_t* block)
{
	ut_ad(block);
	ut_ad(mutex_own(&(buf_pool->mutex)));
	/*将start_lsn设置为0，表示已经将脏页刷入盘中*/
	block->oldest_modification = ut_dulint_zero;
	/*将block从flush list中删除*/
	UT_LIST_REMOVE(flush_list, buf_pool->flush_list, block);

	ut_d(UT_LIST_VALIDATE(flush_list, buf_block_t, buf_pool->flush_list));
	/*修改flush的计数器,这个计数器是记录正在flushing的page数量*/
	(buf_pool->n_flush[block->flush_type]) --;

	if(block->flush_type == BUF_FLUSH_LRU){
		buf_LRU_make_block_old(block); /*将block放入lru old list末尾以便淘汰*/
		buf_pool->LRU_flush_ended ++;
	}

	/*检查批量flush是否完成，如果完成，发送一个完成批量FLUSH的信号*/
	if(buf_pool->n_flush[block->flush_type] == 0 && buf_pool->init_flush[block->flush_type] == FALSE)
		os_event_set(buf_pool->no_flush[block->flush_type]);
}

/*将doublewrite内存中的数据和对应的页刷入disk并且唤醒aio线程,页数据刷入盘是通过异步方式刷入的*/
static void buf_flush_buffered_writes()
{
	buf_block_t*	block;
	ulint			len;
	ulint			i;

	if(trx_doublewrite == NULL){
		os_aio_simulated_wake_handler_threads();
		return ;
	}

	/*这个latch是时间可能比较长，以为涉及到同步IO操作*/
	mutex_enter(&(trx_doublewrite->mutex));
	
	if(trx_doublewrite->first_free == 0){
		mutex_exit(&(trx_doublewrite->mutex));
		return;
	}

	/*确定刷盘数据的长度*/
	if (trx_doublewrite->first_free > TRX_SYS_DOUBLEWRITE_BLOCK_SIZE)
		len = TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE;
	else
		len = trx_doublewrite->first_free * UNIV_PAGE_SIZE;
	/*刷入第一个doublewrite数据块，这里是同步刷入，因为doublewrite是为了保证数据完成不丢失设计的，不能用异步IO刷盘*/
	fil_io(OS_FILE_WRITE, TRUE, TRX_SYS_SPACE,
		trx_doublewrite->block1, 0, len, (void*)trx_doublewrite->write_buf, NULL);

	/*刷入第二个doublewrite数据块*/
	if (trx_doublewrite->first_free > TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
		len = (trx_doublewrite->first_free - TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) * UNIV_PAGE_SIZE;

		fil_io(OS_FILE_WRITE, TRUE, TRX_SYS_SPACE, trx_doublewrite->block2, 0, len,
			(void*)(trx_doublewrite->write_buf + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE), NULL);
	}

	fil_flush(TRX_SYS_SPACE);

	/*将对应的脏页刷入磁盘*/
	for (i = 0; i < trx_doublewrite->first_free; i++) {
		block = trx_doublewrite->buf_block_arr[i];
		/*异步将page刷盘*/
		fil_io(OS_FILE_WRITE | OS_AIO_SIMULATED_WAKE_LATER, FALSE, block->space, block->offset, 0, UNIV_PAGE_SIZE,
			(void*)block->frame, (void*)block);
	}

	os_aio_simulated_wake_handler_threads();

	/*等待aio操作队列信号为空,也就是等待所有的write操作全部完成*/
	os_aio_wait_until_no_pending_writes();

	fil_flush_file_spaces(FIL_TABLESPACE);

	/*保证doublewrite memory中的数据全部刷入磁盘*/
	trx_doublewrite->first_free = 0;

	mutex_exit(&(trx_doublewrite->mutex));
}

/*将block对应的page写入doublewrite中*/
static void buf_flush_post_to_doublewrite_buf(buf_block_t* block)
{
try_again:
	mutex_enter(&(trx_doublewrite->mutex));
	/*doublewrite 数据太多，需要进行强制刷盘*/
	if (trx_doublewrite->first_free >= 2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
			mutex_exit(&(trx_doublewrite->mutex));
			buf_flush_buffered_writes();

			goto try_again;
	}
	/*将block对应的page写入到doublewrite内存中，以便保存*/
	ut_memcpy(trx_doublewrite->write_buf + UNIV_PAGE_SIZE * trx_doublewrite->first_free, block->frame, UNIV_PAGE_SIZE);

	trx_doublewrite->buf_block_arr[trx_doublewrite->first_free] = block;
	trx_doublewrite->first_free ++;

	if (trx_doublewrite->first_free >= 2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
			mutex_exit(&(trx_doublewrite->mutex));
			buf_flush_buffered_writes();

			return;
	}

	mutex_exit(&(trx_doublewrite->mutex));
}

/*将page的LSN和space page_no等信息写入page header/tailer*/
void buf_flush_init_for_writing(byte* page, dulint newest_lsn, ulint space, ulint page_no)
{
	/*将最新page修改的lsn的值写入page的头尾*/
	mach_write_to_8(page + FIL_PAGE_LSN, newest_lsn);
	mach_write_to_8(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN, newest_lsn);

	mach_write_to_4(page + FIL_PAGE_SPACE, space);
	mach_write_to_4(page + FIL_PAGE_OFFSET, page_no);

	/*将页的checksum写入页尾*/
	mach_write_to_4(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN, buf_calc_page_checksum(page));
}

/*将block对应的page的redo log刷入磁盘*/
static void buf_flush_write_block_low(buf_block_t* block)
{
	ut_ad(!ut_dulint_is_zero(block->newest_modification));
	
	/*强制将block->newest_modification作为刷盘点，小于这个值的LSN的redo log全部刷盘*/
	log_flush_up_to(block->newest_modification, LOG_WAIT_ALL_GROUPS);

	buf_flush_init_for_writing(block->frame, block->newest_modification, block->space, block->offset);
	/*没有doublewrite,直接异步刷盘，有可能会造成数据丢失，因为redo log有可能没有刷入磁盘，
	因为redo LOG是按照512为一块计算checksum刷入磁盘的，如果不满512刷盘，那么在redo log读取的时候会校验checksum,
	有可能会将这个块废弃，所以一定要引入doublewrite机制来保证数据不丢失*/
	if (!trx_doublewrite) 
		fil_io(OS_FILE_WRITE | OS_AIO_SIMULATED_WAKE_LATER, FALSE, block->space, block->offset, 0, UNIV_PAGE_SIZE, (void*)block->frame, (void*)block);
	else
		buf_flush_post_to_doublewrite_buf(block);
}

static ulint buf_flush_try_page(ulint space, ulint offset, ulint flush_type)
{
	buf_block_t*	block;
	ibool		locked;

	ut_ad(flush_type == BUF_FLUSH_LRU || flush_type == BUF_FLUSH_LIST || flush_type == BUF_FLUSH_SINGLE_PAGE);

	mutex_enter(&(buf_pool->mutex));

	block = buf_page_hash_get(space, offset);
	/*flush list中的block刷盘*/
	if(flush_type == BUF_FLUSH_LIST && block != NULL && buf_flush_ready_for_flush(block)){
		block->io_fix = BUF_IO_WRITE;
		block->flush_type = flush_type;

		if(buf_pool->no_flush[flush_type] == 0)
			os_event_reset(buf_pool->no_flush[flush_type]);

		(buf_pool->n_flush[flush_type])++;

		locked = FALSE;

		if(block->buf_fix_count == 0){
			rw_lock_s_lock_gen(&(block->lock), BUF_IO_WRITE);
			locked = TRUE;
		}

		mutex_exit(&(buf_pool->mutex));

		/*有IO操作在这个block上，直接刷入doublewrite到磁盘上*/
		if(!locked){
			buf_flush_buffered_writes();
			rw_lock_s_lock_gen(&(block->lock), BUF_IO_WRITE);
		}

		if (buf_debug_prints) {
			printf("Flushing page space %lu, page no %lu \n", block->space, block->offset);
		}

		/*将block对应的page的redo log入盘，并将page写入到doublewrite buf中*/
		buf_flush_write_block_low(block);

		return 1;
	}
	else if(flush_type == BUF_FLUSH_LRU && block != NULL && buf_flush_ready_for_flush(block, flush_type)){
		block->io_fix = BUF_IO_WRITE;
		block->flush_type = flush_type;

		if (buf_pool->n_flush[flush_type] == 0)
			os_event_reset(buf_pool->no_flush[flush_type]);

		(buf_pool->n_flush[flush_type])++;

		rw_lock_s_lock_gen(&(block->lock), BUF_IO_WRITE);

		mutex_exit(&(buf_pool->mutex));

		buf_flush_write_block_low(block);
		/*正真的页数据刷入盘，是在redo log checkpoint建立的时候*/
		return 1;
	}
	else if(flush_type == BUF_FLUSH_SINGLE_PAGE && block != NULL && buf_flush_ready_for_flush(block, flush_type)){
		block->io_fix = BUF_IO_WRITE;
		block->flush_type = flush_type;

		if (buf_pool->n_flush[block->flush_type] == 0)
			os_event_reset(buf_pool->no_flush[block->flush_type]);

		(buf_pool->n_flush[flush_type])++;

		mutex_exit(&(buf_pool->mutex));

		rw_lock_s_lock_gen(&(block->lock), BUF_IO_WRITE);

		if (buf_debug_prints)
			printf("Flushing single page space %lu, page no %lu \n",block->space, block->offset);

		buf_flush_write_block_low(block);
		return 1;
	}
	else{
		mutex_exit(&(buf_pool->mutex));
		return(0);
	}
}

/*将(space, offset)对应的页位置周围的页全部刷入盘中*/
static ulint buf_flush_try_neighbors(ulint space, ulint offset, ulint flush_type)
{
	buf_block_t*	block;
	ulint		low, high;
	ulint		count		= 0;
	ulint		i;

	ut_ad(flush_type == BUF_FLUSH_LRU || flush_type == BUF_FLUSH_LIST);

	low = (offset / BUF_FLUSH_AREA) * BUF_FLUSH_AREA;
	high = (offset / BUF_FLUSH_AREA + 1) * BUF_FLUSH_AREA;

	if(UT_LIST_GET_LEN(buf_pool->LRU) < BUF_LRU_OLD_MIN_LEN){
		low = offset;
		high = offset + 1;
	}
	else if(flush_type == BUF_FLUSH_LIST){
		low = offset;
		high = offset + 1;
	}

	if(high > fil_space_get_size(space))
		high = fil_space_get_size(space);

	mutex_enter(&(buf_pool->mutex));

	for(i = low; i < high; i ++){
		block = buf_page_hash_get(space, i);
		if(block && flush_type == BUF_FLUSH_LRU && i != offset && !block->old)
			continue;

		if(block != NULL && buf_flush_ready_for_flush(block, flush_type)){
			mutex_exit(&(buf_pool->mutex));

			count += buf_flush_try_page(space, i, flush_type);
			mutex_enter(&(buf_pool->mutex));
		}
	}

	mutex_exit(&(buf_pool->mutex));

	return count;
}

/*批量将页刷入磁盘中*/
ulint buf_flush_batch(ulint flush_type, ulint min_n, dulint lsn_limit)
{
	buf_block_t*	block;
	ulint		page_count 	= 0;
	ulint		old_page_count;
	ulint		space;
	ulint		offset;
	ibool		found;

	ut_ad((flush_type == BUF_FLUSH_LRU) || (flush_type == BUF_FLUSH_LIST)); 
	ut_ad((flush_type != BUF_FLUSH_LIST) || sync_thread_levels_empty_gen(TRUE));

	mutex_enter(&(buf_pool->mutex));

	if(buf_pool->n_flush[flush_type] > 0 || buf_pool->init_flush[flush_type] == TRUE){
		mutex_exit(&(buf_pool->mutex));
		return ULINT_UNDEFINED;
	}

	(buf_pool->init_flush)[flush_type] = TRUE;

	for(;;){
		if(page_count >= min_n)
			break;

		if(flush_type == BUF_FLUSH_LRU)
			block = UT_LIST_GET_LAST(buf_pool->LRU);
		else{
			ut_ad(flush_type == BUF_FLUSH_LIST);
			block = UT_LIST_GET_LAST(buf_pool->flush_list);

			if(block != NULL || ut_dulint_cmp(block->oldest_modification, lsn_limit) >= 0)
				break;
		}

		found = FALSE;
		/*尝试将周围批量flush*/
		while(block != NULL && !found){
			if(buf_flush_ready_for_flush(block, flush_type)){
				found = TRUE;
				space = block->space;
				offset = block->offset;

				mutex_exit(&(buf_pool->mutex));

				old_page_count = page_count;
				page_count += buf_flush_try_neighbors(space, offset, flush_type);
				mutex_enter(&(buf_pool->mutex));
			}
			else if(flush_type == BUF_FLUSH_LRU)
				block = UT_LIST_GET_PREV(LRU, block);
			else {
				ut_ad(flush_type == BUF_FLUSH_LIST);
				block = UT_LIST_GET_PREV(flush_type, block);
			}
		}

		if(!found)
			break;
	}

	/*设置批量刷入的信号*/
	(buf_pool->init_flush)[flush_type] = FALSE;
	if ((buf_pool->n_flush[flush_type] == 0) && (buf_pool->init_flush[flush_type] == FALSE)){
		os_event_set(buf_pool->no_flush[flush_type]);
	}

	mutex_exit(&(buf_pool->mutex));
	
	buf_flush_buffered_writes();

	if (buf_debug_prints && page_count > 0) {
		if (flush_type == BUF_FLUSH_LRU)
			printf("Flushed %lu pages in LRU flush\n", page_count);
		else if (flush_type == BUF_FLUSH_LIST)
			printf("Flushed %lu pages in flush list flush\n",
				page_count);
		else 
			ut_error;
	}

	return page_count;
}

/*对一个pages batch flush等待其完成*/
void buf_flush_wait_batch_end(ulint type)
{
	ut_ad((type == BUF_FLUSH_LRU) || (type == BUF_FLUSH_LIST));
	os_event_wait(buf_pool->no_flush[type]);
}

/*计算最大可以同时刷盘的LRU中的page的个数*/
static ulint buf_flush_LRU_recommendation()
{
	buf_block_t*	block;
	ulint		n_replaceable;
	ulint		distance	= 0;

	mutex_enter(&(buf_pool->mutex));

	n_replaceable = UT_LIST_GET_LEN(buf_pool->free);
	block = UT_LIST_GET_LAST(buf_pool->LRU);

	while(block != NULL && n_replaceable < BUF_FLUSH_FREE_BLOCK_MARGIN + BUF_FLUSH_EXTRA_MARGIN
		&& distance < BUF_LRU_FREE_SEARCH_LEN){
			if(buf_flush_ready_for_replace(block))
				n_replaceable ++;

			distance ++;
			block = UT_LIST_GET_PREV(LRU, block);
	}

	mutex_exit(&(buf_pool->mutex));

	if(n_replaceable >= BUF_FLUSH_FREE_BLOCK_MARGIN)
		return 0;

	return (BUF_FLUSH_FREE_BLOCK_MARGIN + BUF_FLUSH_EXTRA_MARGIN - n_replaceable);
}

void buf_flush_free_margin()
{
	ulint n_to_flush = buf_flush_LRU_recommendation();
	if(n_to_flush > 0)
		buf_flush_batch(BUF_FLUSH_LRU, n_to_flush, ut_dulint_zero);
}

/*检查block的start_lsn的顺序*/
static ibool buf_flush_validate_low(void)
{
	buf_block_t*	block;
	dulint		om;

	UT_LIST_VALIDATE(flush_list, buf_block_t, buf_pool->flush_list);

	block = UT_LIST_GET_FIRST(buf_pool->flush_list);

	while (block != NULL) {
		om = block->oldest_modification;
		ut_a(block->state == BUF_BLOCK_FILE_PAGE);
		ut_a(ut_dulint_cmp(om, ut_dulint_zero) > 0);

		block = UT_LIST_GET_NEXT(flush_list, block);

		if (block)
			ut_a(ut_dulint_cmp(om, block->oldest_modification) >= 0);
	}

	return(TRUE);
}

ibool buf_flush_validate()
{
	ibool	ret;

	mutex_enter(&(buf_pool->mutex));

	ret = buf_flush_validate_low();

	mutex_exit(&(buf_pool->mutex));

	return(ret);
}

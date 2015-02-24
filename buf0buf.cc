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

/*初始化一个buf_block*/
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

/*创建buf_poot_t对象*/
static buf_pool_t* buf_pool_create(ulint max_size, ulint curr_size)
{
	byte*			frame;
	ulint			i;
	buf_block_t*	block;

	ut_a(max_size == curr_size);

	buf_pool = mem_alloc(sizeof(buf_pool_t));

	/*初始化常规参数*/
	mutex_create(&(buf_pool->mutex));
	mutex_set_level(&(buf_pool->mutex), SYNC_BUF_POOL);

	mutex_enter(&(uf_pool->mutex));

	/*开辟一块max_size个page size大小的内存作为缓冲池,这里的+1是为了UNIV_PAGE_SIZE对齐*/
	buf_pool->frame_mem = ut_malloc(UNIV_PAGE_SIZE * (max_size + 1));
	if(buf_pool->frame_mem == NULL)
		return NULL;

	/*开辟一个长度为max_size的block数组*/
	buf_pool->blocks = ut_malloc(max_size * sizeof(buf_block_t));
	if(buf_pool->blocks == NULL)
		return NULL;

	buf_pool->max_size = max_size;
	buf_pool->curr_size = curr_size;

	/*在frame_mem上分配一个frame page,作为frame_zero*/
	frame = ut_align(buf_pool->frame_mem, UNIV_PAGE_SIZE);
	buf_pool->frame_zero = frame;
	buf_pool->high_end = frame + UNIV_PAGE_SIZE * curr_size;

	/*对所有block进行初始化，并建立与各个frame之间的关系*/
	for(i = 0; i < max_size; i ++){
		block = buf_pool_get_nth_block(buf_pool, i);
		buf_block_init(block, frame);
		frame = frame + UNIV_PAGE_SIZE;
	}

	buf_pool->page_hash = hash_create(2 * max_size);
	buf_pool->n_pend_reads = 0;
	buf_pool->last_printout_time = time(NULL);

	buf_pool->n_pages_read = 0;
	buf_pool->n_pages_written = 0;
	buf_pool->n_pages_created = 0;

	buf_pool->n_page_gets = 0;
	buf_pool->n_page_gets_old = 0;
	buf_pool->n_pages_read_old = 0;
	buf_pool->n_pages_written_old = 0;
	buf_pool->n_pages_created_old = 0;

	/*对flush list做初始化*/
	UT_LIST_INIT(buf_pool->flush_list);
	for(i = BUF_FLUSH_LRU; i <= BUF_FLUSH_LIST; i ++){
		buf_pool->n_flush[i] = 0;
		buf_pool->init_flush[i] =  FALSE;
		buf_pool->no_flush[i] = os_event_create(NULL);
	}

	buf_pool->LRU_flush_ended = 0;
	buf_pool->ulint_clock = 1;
	buf_pool->freed_page_clock = 0;

	/*对LRU LIST初始化*/
	UT_LIST_INIT(buf_pool->LRU);
	buf_pool->LRU_old = 0;

	UT_LIST_INIT(buf_pool->free);
	for(i = 0; i < curr_size; i ++){
		block = buf_pool_get_nth_block(buf_pool, i);
		memset(block->frame, '\0', UNIV_PAGE_SIZE);

		UT_LIST_ADD_FIRST(free, buf_pool->free, block);
	}

	mutex_exit(&(buf_pool->mutex));
	/*建立自适应HASH索引*/
	btr_search_sys_create(curr_size * UNIV_PAGE_SIZE / sizeof(void*) / 64);

	return buf_pool;
}

/*初始化缓冲池，一般在MYSQL启动的时候调用*/
void buf_pool_init(ulint max_size, ulint curr_size)
{
	ut_a(buf_pool == NULL);

	buf_pool_create(max_size, curr_size);

	ut_ad(buf_validate());
}

/*分配一个buf block*/
UNIV_INLINE buf_block_t* buf_block_alloc()
{
	return buf_LRU_get_free_block();
}

/*将block从old LRU LIST移到young中，就是LRU List的开始位置*/
UNIV_INLINE void buf_block_make_young(buf_block_t* block)
{
	if(buf_pool->freed_page_clock >= block->freed_page_clock + 1 + (buf_pool->curr_size / 1024))
		buf_LRU_make_block_young(block);
}

/*释放一个block*/
UNIV_INLINE void buf_block_free(buf_block_t* block)
{
	ut_ad(block->state != BUF_BLOCK_FILE_PAGE);

	mutex_enter(&(buf_pool->mutex));
	buf_LRU_block_free_non_file_page(block);
	mutex_exit(&(buf_pool->mutex));
}

/*分配一个buffer frame*/
buf_frame_t* buf_frame_alloc()
{
	return buf_block_alloc()->frame;
}
/*释放一个buffer frame*/
void buf_frame_free(buf_frame_t* frame)
{
	buf_block_free(buf_block_align(frame));
}

/*通过space id和page no定位到对应的buf_block*/
buf_block_t* buf_page_peek_block(ulint space, ulint offset)
{
	buf_block_t* block;

	mutex_enter_fast(&(buf_pool->mutex));
	block = buf_page_hash_get(space, offset);
	mutex_exit(&(buf_pool->mutex));
}

/*通过space id和page no查找对应的page是否有哈希索引*/
ibool buf_page_peek_if_search_hashed(ulint space, ulint offset)
{
	buf_block_t* block;
	ibool is_hashed;

	mutex_enter_fast(&(buf_pool->mutex));
	
	block = buf_page_hash_get(space, offset);
	if(block != NULL)
		is_hashed = FALSE;
	else
		is_hashed = block->is_hashed;

	mutex_exit(&(buf_pool->mutex));

	return is_hashed;
}

/*判断space id与page no对应的page是否在buf pool上有缓冲*/
ibool buf_page_peek(ulint space, ulint offset)
{
	if(buf_page_peek_block(space, offset))
		return TRUE;

	return FALSE;
}
/*设置block->file_page_was_freed为TRUE*/
buf_block_t* buf_page_set_file_page_was_freed(ulint space, ulint offset)
{
	buf_block_t* block;

	mutex_enter_fast(&(buf_pool->mutex));

	block = buf_page_hash_get(space, offset);
	if(block)
		block->file_page_was_freed = TRUE;

	mutex_exit(&(buf_pool->mutex));

	return block;
}
/*设置block->file_page_was_freed为FALSE*/
buf_block_t* buf_page_reset_file_page_was_freed(ulint space, ulint offset)
{
	buf_block_t*	block;

	mutex_enter_fast(&(buf_pool->mutex));

	block = buf_page_hash_get(space, offset);
	if (block) 
		block->file_page_was_freed = FALSE;

	mutex_exit(&(buf_pool->mutex));

	return(block);
}

/*通过space id和page no获得对应page在buf_pool中的frame地址，在这一过程可能会触发page从磁盘导入到buf_pool中*/
buf_frame_t* buf_page_get_gen(ulint space, ulint offset, ulint rw_latch, buf_frame_t* guess, ulint mode, char* file, ulint line, mtr_t* mtr)
{
	buf_block_t*	block;
	ibool		accessed;
	ulint		fix_type;
	ibool		success;
	ibool		must_read;

	ut_ad(mtr);
	ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH) || (rw_latch == RW_NO_LATCH));
	ut_ad((mode != BUF_GET_NO_LATCH) || (rw_latch == RW_NO_LATCH));
	ut_ad((mode == BUF_GET) || (mode == BUF_GET_IF_IN_POOL) || (mode == BUF_GET_NO_LATCH) || (mode == BUF_GET_NOWAIT));

	buf_pool->n_page_gets ++;

loop:
	mutex_enter_fast(&(buf_pool->mutex));
	
	block = NULL;
	if(guess){
		block = buf_block_align(guess);
		/*block和对应的space id和page no不匹配*/
		if(offset != block->offset || space != block->space || block->state != BUF_BLOCK_FILE_PAGE)
			block = NULL;
	}

	/*在buf_pool->hash_table中找*/
	if(block == NULL)
		block = buf_page_hash_get(space, offset);

	/*page 不在缓冲池中*/
	if(block == NULL){
		mutex_exit(&(buf_pool->mutex));
		if(mode == BUF_GET_IF_IN_POOL)
			return NULL;

		/*从磁盘上读入对应的页到buf pool中*/
		buf_read_page(space, offset);

		goto loop;
	}

	must_read = FALSE;
	if(block->io_fix == BUF_IO_READ){ /*IO读操作*/
		must_read = TRUE;
		if(mode == BUF_GET_IF_IN_POOL){
			mutex_exit(&(buf_pool->mutex));
			return NULL;
		}
	}

	buf_block_buf_fix_inc(block);

	buf_block_make_young(block);

	accessed = block->accessed;
	block->accessed = TRUE;

	mutex_exit(&(buf_pool->mutex));

	ut_ad(block->buf_fix_count > 0);
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);

	/*对block上latch锁*/
	if(mode == BUF_GET_NOWAIT){
		if(rw_latch == RW_S_LATCH){
			success = rw_lock_s_lock_func_nowait(&(block->lock), file, line);
			fix_type = MTR_MEMO_PAGE_S_FIX;
		}
		else{
			ut_ad(rw_latch == RW_X_LATCH);
			success = rw_lock_x_lock_func_nowait(&(block->lock), file, line);
			fix_type = MTR_MEMO_PAGE_X_FIX;
		}

		/*上latch lock失败*/
		if(!success){
			mutex_enter(&(buf_pool->mutex));
			block->buf_fix_count--;
			mutex_exit(&(buf_pool->mutex));

			return NULL;
		}
	}
	else if(rw_latch == RW_NO_LATCH){
		if (must_read) {
			rw_lock_x_lock(&(block->read_lock));
			rw_lock_x_unlock(&(block->read_lock));
		}
		fix_type = MTR_MEMO_BUF_FIX;
	}
	else if(rw_latch == RW_S_LATCH){
		rw_lock_s_lock_func(&(block->lock), 0, file, line);
		fix_type = MTR_MEMO_PAGE_S_FIX;
	}
	else{
		rw_lock_x_lock_func(&(block->lock), 0, file, line);
		fix_type = MTR_MEMO_PAGE_X_FIX;
	}

	mtr_memo_push(mtr, block, fix_type);

	/*尝试预读，因为block是第一次读取到buffer pool中*/
	if(!accessed)
		buf_read_ahead_linear(space, offset);

	return block->frame;
}

/*判断是否可以用乐观方式(当前page不从磁盘读取)访问一个page*/
ibool buf_page_optimistic_get_func(ulint rw_latch, buf_frame_t* guess, dulint modify_clock, char* file, ulint line, mtr_t* mtr)
{
	buf_block_t*	block;
	ibool		accessed;
	ibool		success;
	ulint		fix_type;

	ut_ad(mtr && guess);
	ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH));

	buf_pool->n_page_gets ++;

	block = buf_block_align(guess);
	
	mutex_enter(&(buf_pool->mutex));

	/*不是BLOK FILE PAGE*/
	if(block->state != BUF_BLOCK_FILE_PAGE){
		mutex_exit(&(buf_pool->mutex));
		return FALSE;
	}

	buf_block_buf_fix_inc(block);

	buf_block_make_young(block);

	accessed = block->accessed;
	block->accessed = TRUE;

	ut_ad(!ibuf_inside() || ibuf_page(block->space, block->offset));

	/*对block上latch锁*/
	if (rw_latch == RW_S_LATCH) {
		success = rw_lock_s_lock_func_nowait(&(block->lock), file, line);
		fix_type = MTR_MEMO_PAGE_S_FIX;
	} else {
		success = rw_lock_x_lock_func_nowait(&(block->lock),file, line);
		fix_type = MTR_MEMO_PAGE_X_FIX;
	}

	if(!success){
		mutex_enter(&(buf_pool->mutex));
		block->buf_fix_count --;
		mutex_exit(&(buf_pool->mutex));

		return FALSE;
	}

	/*modify_clock不匹配*/
	if(!UT_DULINT_EQ(modify_clock, block->modify_clock)){
		buf_page_dbg_add_level(block->frame, SYNC_NO_ORDER_CHECK);
		/*释放持有的latch locker*/
		if(rw_latch == RW_S_LATCH)
			rw_lock_s_unlock(&(block->lock));
		else
			rw_lock_x_unlock(&(block->lock));

		block->buf_fix_count --;

		mutex_exit(&(buf_pool->mutex));

		return FALSE;
	}

	mtr_memo_push(mtr, block, fix_type);

	ut_ad(block->buf_fix_count > 0);
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);

	/*进行预读*/
	if(!accessed)
		buf_read_ahead_linear(buf_frame_get_space_id(guess), buf_frame_get_page_no(guess));

	return TRUE;
}

/*判断是否可以用nowait方式访问一个已知的page*/
ibool buf_page_get_known_nowait(ulint rw_latch, buf_frame_t* guess, ulint mode, char* file, ulint line, mtr_t* mtr)
{
	buf_block_t*	block;
	ibool		success;
	ulint		fix_type;

	ut_ad(mtr);
	ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH));

	buf_pool->n_page_gets ++;
	block = buf_block_align(guess);

	mutex_enter(&(buf_pool->mutex));
	
	/*其他线程可能刚刚已经把这个buf block从LRU list中删除了,无法做到无等待访问page*/
	if(block->state == BUF_BLOCK_REMOVE_HASH){
		mutex_exit(&(buf_pool->mutex));
		return FALSE;
	}

	buf_block_buf_fix_inc(block);

	if(mode == BUF_MAKE_YOUNG)
		buf_block_make_young(block);

	mutex_exit(&(buf_pool->mutex));

	ut_ad(!ibuf_inside() || mode == BUF_KEEP_OLD);

	if (rw_latch == RW_S_LATCH) {
		success = rw_lock_s_lock_func_nowait(&(block->lock), file, line);
		fix_type = MTR_MEMO_PAGE_S_FIX;
	} else {
		success = rw_lock_x_lock_func_nowait(&(block->lock), file, line);
		fix_type = MTR_MEMO_PAGE_X_FIX;
	}
	/*上锁失败*/
	if(!success){
		mutex_enter(&(buf_pool->mutex));
		block->buf_fix_count--;
		mutex_exit(&(buf_pool->mutex));

		return FALSE;
	}

	mtr_memo_push(mtr, block, fix_type);

	ut_ad(block->buf_fix_count > 0);
	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);

	return TRUE;
}

/*为从ibbackup恢复page而进行的block初始化*/
void buf_page_init_for_backup_restore(ulint space, ulint offset, buf_block_t* block)
{
	/* Set the state of the block */
	block->magic_n		= BUF_BLOCK_MAGIC_N;

	block->state 		= BUF_BLOCK_FILE_PAGE;
	block->space 		= space;
	block->offset 		= offset;

	block->lock_hash_val	= 0;
	block->lock_mutex	= NULL;

	block->freed_page_clock = 0;

	block->newest_modification = ut_dulint_zero;
	block->oldest_modification = ut_dulint_zero;

	block->accessed		= FALSE;
	block->buf_fix_count 	= 0;
	block->io_fix		= 0;

	block->n_hash_helps	= 0;
	block->is_hashed	= FALSE;
	block->n_fields         = 1;
	block->n_bytes          = 0;
	block->side             = BTR_SEARCH_LEFT_SIDE;

	block->file_page_was_freed = FALSE;
}

/*初始化一个buffer pool page*/
static void buf_page_init(ulint space, ulint offset, buf_block_t* block)
{
	ut_ad(mutex_own(&(buf_pool->mutex)));
	ut_ad(block->state == BUF_BLOCK_READY_FOR_USE);

	block->magic_n		= BUF_BLOCK_MAGIC_N;

	block->state 		= BUF_BLOCK_FILE_PAGE;
	block->space 		= space;
	block->offset 		= offset;

	block->lock_hash_val	= lock_rec_hash(space, offset);
	block->lock_mutex	= NULL;

	/*将space page_no与block在page_hash中建立对应关系*/
	HASH_INSERT(buf_block_t, hash, buf_pool->page_hash, buf_page_address_fold(space, offset), block);

	block->freed_page_clock = 0;

	block->newest_modification = ut_dulint_zero;
	block->oldest_modification = ut_dulint_zero;

	block->accessed		= FALSE;
	block->buf_fix_count 	= 0;
	block->io_fix		= 0;

	block->n_hash_helps	= 0;
	block->is_hashed	= FALSE;
	block->n_fields     = 1;
	block->n_bytes      = 0;
	block->side         = BTR_SEARCH_LEFT_SIDE;

	block->file_page_was_freed = FALSE;
}

/************************************************************************
Function which inits a page for read to the buffer buf_pool. If the page is
already in buf_pool, does nothing. Sets the io_fix flag to BUF_IO_READ and
sets a non-recursive exclusive lock on the buffer frame. The io-handler must
take care that the flag is cleared and the lock released later. This is one
of the functions which perform the state transition NOT_USED => FILE_PAGE to
a block (the other is buf_page_create). 
**************************************************************************/ 
buf_block_t* buf_page_init_for_read(ulint mode, ulint space, ulint offset)
{
	buf_block_t*	block;
	mtr_t			mtr;

	if(mode == BUF_READ_IBUF_PAGES_ONLY){
		ut_ad(!ibuf_bitmap_page(offset));
		ut_ad(ibuf_inside());

		mtr_start(&mtr);
		/*page不是ibuf的一个page,提交mini transction*/
		if(!ibuf_page_low(space, offset, &mtr)){
			mtr_commit(&mtr);
			return NULL;
		}
	}
	else
		ut_ad(mode == BUF_READ_ANY_PAGE);

	/*在buf pool上分配一个block*/
	block = buf_block_alloc();
	ut_ad(block);

	mutex_enter(&(buf_pool->mutex));
	/*在自适应hash索引上已经存在同样一个block,说明这个page已经在缓冲池中,直接释放返回*/
	if(NULL != buf_page_hash_get(space, offset)){
		mutex_exit(&(buf_pool->mutex));
		buf_block_free(block);

		if(mode == BUF_READ_IBUF_PAGES_ONLY)
			mtr_commit(&mtr);

		return NULL;
	}

	ut_ad(block);

	/*对page对应的block进行初始化*/
	buf_page_init(space, offset, block);
	/*将block压入LRU List的old区域*/
	buf_LRU_add_block(block, TRUE); 

	block->io_fix = BUF_IO_READ;
	block->n_pend_reads ++;

	/*在buf_page_io_complete时会将lock和read_lock释放*/
	rw_lock_x_lock_gen(&(block->lock), BUF_IO_READ);
	rw_lock_x_lock_gen(&(block->read_lock), BUF_IO_READ);

	mutex_exit(&(buf_pool->mutex));

	if(mode == BUF_READ_IBUF_PAGES_ONLY)
		mtr_commit(&mtr);

	return block;
}

/*建立一个page与buf_pool block之间的对应关系，并且会放入LRU队列中，通常这个过程是不需要从磁盘中导入页数据的，
一般是将block state有NO_USED-->FILE_PAGE*/
buf_frame_t* buf_page_create(ulint space, ulint offset, mtr_t* mtr)
{
	buf_frame_t*	frame;
	buf_block_t*	block;
	buf_block_t*	free_block	= NULL;

	ut_ad(mtr);

	free_block = buf_LRU_get_free_block();

	/*进行ibuf中数据记录的删除,只是个初始化过程，不是一个从磁盘导入页数据的过程
	Delete possible entries for the page from the insert buffer:
	such can exist if the page belonged to an index which was dropped*/
	ibuf_merge_or_delete_for_page(NULL, space, offset);

	mutex_enter(&(buf_pool->mutex));

	block = buf_page_hash_get(space, offset);
	if(block != NULL){
		block->file_page_was_freed = FALSE;
		
		mutex_exit(&(buf_pool->mutex));
		buf_block_free(block);

		/*数据页已经在buf pool当中，直接返回frame对象*/
		frame = buf_page_get_with_no_latch(space, offset, mtr);

		return frame;
	}

	if(buf_debug_prints)
		printf("Creating space %lu page %lu to buffer\n", space, offset);

	block = free_block;
	/*初始化page与block隐射关系，并加入到LRU中*/
	buf_page_init(space, offset, block);
	buf_LRU_add_block(block, FALSE);

	buf_block_buf_fix_inc(block);

	mtr_memo_push(mtr, block, MTR_MEMO_BUF_FIX);

	block->accessed = TRUE;
	buf_pool->n_pages_created ++;

	mutex_exit(&(buf_pool->mutex));

	frame = block->frame;

	return frame;
}

/*从磁盘上读或者写一个页数据完成*/
void buf_page_io_complete(buf_block_t* block)
{
	dict_index_t*	index;
	dulint		id;
	ulint		io_type;
	ulint		read_page_no;

	ut_ad(block);

	io_type = block->io_fix;
	if(io_type == BUF_IO_READ){
		read_page_no = mach_read_from_4(block->frame + FIL_PAGE_OFFSET);
		if(read_page_no != 0 && trx_doublewrite_age_inside(read_page_no) && read_page_no != block->offset){
			fprintf(stderr,"InnoDB: Error: page n:o stored in the page read in is %lu, should be %lu!\n",
			read_page_no, block->offset);
		}

		/*判断页是否完整*/
		if(buf_page_is_corrupted(block->frame)){
			fprintf(stderr,
				"InnoDB: Database page corruption on disk or a failed\n"
				"InnoDB: file read of page %lu.\n", block->offset);

			fprintf(stderr, "InnoDB: You may have to recover from a backup.\n");

			buf_page_print(block->frame);

			fprintf(stderr, "InnoDB: Database page corruption on disk or a failed\n"
				"InnoDB: file read of page %lu.\n", block->offset);

			fprintf(stderr, "InnoDB: You may have to recover from a backup.\n");
			fprintf(stderr,
				"InnoDB: It is also possible that your operating\n"
				"InnoDB: system has corrupted its own file cache\n"
				"InnoDB: and rebooting your computer removes the\n"
				"InnoDB: error.\n"
				"InnoDB: If the corrupt page is an index page\n"
				"InnoDB: you can also try to fix the corruption\n"
				"InnoDB: by dumping, dropping, and reimporting\n"
				"InnoDB: the corrupt table. You can use CHECK\n"
				"InnoDB: TABLE to scan your table for corruption.\n"
				"InnoDB: Look also at section 6.1 of\n"
				"InnoDB: http://www.innodb.com/ibman.html about\n"
				"InnoDB: forcing recovery.\n");

			if(srv_force_recovery < SRV_FORCE_IGNORE_CORRUPT){
				fprintf(stderr, "InnoDB: Ending processing because of a corrupt database page.\n");
				exit(1);
			}
		}

		/*正在redo log恢复过程*/
		if(recv_recovery_is_on())
			recv_recover_page(FALSE, TRUE, block->frame, block->space, block->offset);

		/*进行ibuf中的记录合并*/
		if(!recv_no_ibuf_operations)
			ibuf_merge_or_delete_for_page(block->frame, block->space, block->offset);
	}

	mutex_enter(&(buf_pool->mutex));

	/*更新block与buf_pool的计数状态*/
	block->io_fix = 0;
	if(io_type == BUF_IO_READ){
		ut_ad(buf_pool->n_pend_reads > 0);

		buf_pool->n_pend_reads--;
		buf_pool->n_pages_read++;

		/*在发起读的时候进行也lock*/
		rw_lock_x_unlock_gen(&(block->lock), BUF_IO_READ);
		rw_lock_x_unlock_gen(&(block->read_lock), BUF_IO_READ);

		if(buf_debug_prints)
			printf("Has read ");
	}
	else{
		ut_ad(io_type == BUF_IO_WRITE);
		/*page磁盘写完成*/
		buf_flush_write_complete(block);

		rw_lock_s_unlock_gen(&(block->lock), BUF_IO_WRITE);

		buf_pool->n_pages_written ++;

		if(buf_debug_prints)
			printf("Has written ");
	}

	mutex_exit(&(buf_pool->mutex));

	if (buf_debug_prints) {
		printf("page space %lu page no %lu", block->space, block->offset);
		id = btr_page_get_index_id(block->frame);

		index = NULL;

		printf("\n");
	}
}

void buf_pool_invalidate(void)
{
	ibool	freed;

	ut_ad(buf_all_freed());

	freed = TRUE;
	while (freed)
		freed = buf_LRU_search_and_free_block(0);

	mutex_enter(&(buf_pool->mutex));

	ut_ad(UT_LIST_GET_LEN(buf_pool->LRU) == 0);

	mutex_exit(&(buf_pool->mutex));
}

ibool buf_validate(void)
{
	buf_block_t*	block;
	ulint		i;
	ulint		n_single_flush	= 0;
	ulint		n_lru_flush	= 0;
	ulint		n_list_flush	= 0;
	ulint		n_lru		= 0;
	ulint		n_flush		= 0;
	ulint		n_free		= 0;
	ulint		n_page		= 0;

	ut_ad(buf_pool);

	mutex_enter(&(buf_pool->mutex));

	for (i = 0; i < buf_pool->curr_size; i++) {

		block = buf_pool_get_nth_block(buf_pool, i);

		if (block->state == BUF_BLOCK_FILE_PAGE) {
			ut_a(buf_page_hash_get(block->space, block->offset) == block);
			n_page++;

			if (block->io_fix == BUF_IO_WRITE) {
				if (block->flush_type == BUF_FLUSH_LRU){
					n_lru_flush++;
					ut_a(rw_lock_is_locked(&(block->lock), RW_LOCK_SHARED));
				}
				else if (block->flush_type == BUF_FLUSH_LIST)
						n_list_flush++;
				else if (block->flush_type == BUF_FLUSH_SINGLE_PAGE)
						n_single_flush++;
				else
					ut_error;
			} 
			else if (block->io_fix == BUF_IO_READ)
				ut_a(rw_lock_is_locked(&(block->lock), RW_LOCK_EX));

			n_lru++;

			if (ut_dulint_cmp(block->oldest_modification, ut_dulint_zero) > 0)
					n_flush++;

		} else if (block->state == BUF_BLOCK_NOT_USED) {
			n_free++;
		}
	}

	if (n_lru + n_free > buf_pool->curr_size) {
		printf("n LRU %lu, n free %lu\n", n_lru, n_free);
		ut_error;
	}

	ut_a(UT_LIST_GET_LEN(buf_pool->LRU) == n_lru);
	if (UT_LIST_GET_LEN(buf_pool->free) != n_free) {
		printf("Free list len %lu, free blocks %lu\n", UT_LIST_GET_LEN(buf_pool->free), n_free);
		ut_error;
	}
	ut_a(UT_LIST_GET_LEN(buf_pool->flush_list) == n_flush);

	ut_a(buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE] == n_single_flush);
	ut_a(buf_pool->n_flush[BUF_FLUSH_LIST] == n_list_flush);
	ut_a(buf_pool->n_flush[BUF_FLUSH_LRU] == n_lru_flush);

	mutex_exit(&(buf_pool->mutex));

	ut_a(buf_LRU_validate());
	ut_a(buf_flush_validate());

	return(TRUE);
}	

void buf_print(void)
{
	dulint*		index_ids;
	ulint*		counts;
	ulint		size;
	ulint		i;
	ulint		j;
	dulint		id;
	ulint		n_found;
	buf_frame_t* 	frame;
	dict_index_t*	index;

	ut_ad(buf_pool);

	size = buf_pool_get_curr_size() / UNIV_PAGE_SIZE;

	index_ids = mem_alloc(sizeof(dulint) * size);
	counts = mem_alloc(sizeof(ulint) * size);

	mutex_enter(&(buf_pool->mutex));

	printf("buf_pool size %lu \n", size);
	printf("database pages %lu \n", UT_LIST_GET_LEN(buf_pool->LRU));
	printf("free pages %lu \n", UT_LIST_GET_LEN(buf_pool->free));
	printf("modified database pages %lu \n", UT_LIST_GET_LEN(buf_pool->flush_list));

	printf("n pending reads %lu \n", buf_pool->n_pend_reads);

	printf("n pending flush LRU %lu list %lu single page %lu\n",
		buf_pool->n_flush[BUF_FLUSH_LRU],
		buf_pool->n_flush[BUF_FLUSH_LIST], buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE]);

	printf("pages read %lu, created %lu, written %lu\n",
		buf_pool->n_pages_read, buf_pool->n_pages_created, buf_pool->n_pages_written);

	/* Count the number of blocks belonging to each index in the buffer */

	n_found = 0;

	for (i = 0 ; i < size; i++)
		counts[i] = 0;

	for (i = 0; i < size; i++) {
		frame = buf_pool_get_nth_block(buf_pool, i)->frame;

		if (fil_page_get_type(frame) == FIL_PAGE_INDEX) {
			id = btr_page_get_index_id(frame);
			/* Look for the id in the index_ids array */
			j = 0;

			while (j < n_found){
				if (ut_dulint_cmp(index_ids[j], id) == 0){
					(counts[j])++;
					break;
				}
				j++;
			}

			if (j == n_found) {
				n_found++;
				index_ids[j] = id;
				counts[j] = 1;
			}
		}
	}

	mutex_exit(&(buf_pool->mutex));

	for (i = 0; i < n_found; i++) {
		index = dict_index_get_if_in_cache(index_ids[i]);

		printf("Block count for index %lu in buffer is about %lu",
			ut_dulint_get_low(index_ids[i]), counts[i]);

		if (index)
			printf(" index name %s table %s", index->name, index->table->name);

		printf("\n");
	}

	mem_free(index_ids);
	mem_free(counts);

	ut_a(buf_validate());
}

ulint buf_get_n_pending_ios(void)
{
	return(buf_pool->n_pend_reads + buf_pool->n_flush[BUF_FLUSH_LRU]
	+ buf_pool->n_flush[BUF_FLUSH_LIST] + buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE]);
}

void
buf_print_io(char*	buf,	/* in/out: buffer where to print */
	         char*	buf_end)/* in: buffer end */
{
	time_t	current_time;
	double	time_elapsed;
	ulint	size;
	
	ut_ad(buf_pool);

	if (buf_end - buf < 400) {

		return;
	}

	size = buf_pool_get_curr_size() / UNIV_PAGE_SIZE;

	mutex_enter(&(buf_pool->mutex));
	
	buf += sprintf(buf,
		"Buffer pool size   %lu\n", size);
	buf += sprintf(buf,
		"Free buffers       %lu\n", UT_LIST_GET_LEN(buf_pool->free));
	buf += sprintf(buf,
		"Database pages     %lu\n", UT_LIST_GET_LEN(buf_pool->LRU));

	buf += sprintf(buf,
		"Modified db pages  %lu\n",
				UT_LIST_GET_LEN(buf_pool->flush_list));

	buf += sprintf(buf, "Pending reads %lu \n", buf_pool->n_pend_reads);

	buf += sprintf(buf,
		"Pending writes: LRU %lu, flush list %lu, single page %lu\n",
		buf_pool->n_flush[BUF_FLUSH_LRU],
		buf_pool->n_flush[BUF_FLUSH_LIST],
		buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE]);

	current_time = time(NULL);
	time_elapsed = 0.001 + difftime(current_time,
						buf_pool->last_printout_time);
	buf_pool->last_printout_time = current_time;

	buf += sprintf(buf, "Pages read %lu, created %lu, written %lu\n",
			buf_pool->n_pages_read, buf_pool->n_pages_created,
						buf_pool->n_pages_written);
	buf += sprintf(buf, "%.2f reads/s, %.2f creates/s, %.2f writes/s\n",
		(buf_pool->n_pages_read - buf_pool->n_pages_read_old) / time_elapsed,
		(buf_pool->n_pages_created - buf_pool->n_pages_created_old)/ time_elapsed,
		(buf_pool->n_pages_written - buf_pool->n_pages_written_old)/ time_elapsed);

	if (buf_pool->n_page_gets > buf_pool->n_page_gets_old) {
		buf += sprintf(buf, "Buffer pool hit rate %lu / 1000\n",
		1000 - ((1000 * (buf_pool->n_pages_read - buf_pool->n_pages_read_old)) / (buf_pool->n_page_gets - buf_pool->n_page_gets_old)));
	} 
	else
		buf += sprintf(buf, "No buffer pool activity since the last printout\n");

	buf_pool->n_page_gets_old = buf_pool->n_page_gets;
	buf_pool->n_pages_read_old = buf_pool->n_pages_read;
	buf_pool->n_pages_created_old = buf_pool->n_pages_created;
	buf_pool->n_pages_written_old = buf_pool->n_pages_written;

	mutex_exit(&(buf_pool->mutex));
}

void buf_refresh_io_stats(void)
{
	buf_pool->last_printout_time = time(NULL);
	buf_pool->n_page_gets_old = buf_pool->n_page_gets;
	buf_pool->n_pages_read_old = buf_pool->n_pages_read;
	buf_pool->n_pages_created_old = buf_pool->n_pages_created;
	buf_pool->n_pages_written_old = buf_pool->n_pages_written;
}

ibool buf_all_freed(void)
{
	buf_block_t*	block;
	ulint		i;
	
	ut_ad(buf_pool);

	mutex_enter(&(buf_pool->mutex));

	for (i = 0; i < buf_pool->curr_size; i++) {
		block = buf_pool_get_nth_block(buf_pool, i);

		if (block->state == BUF_BLOCK_FILE_PAGE) {
			if (!buf_flush_ready_for_replace(block))
			    	ut_error;

		}
 	}

	mutex_exit(&(buf_pool->mutex));

	return(TRUE);
}

/* out: TRUE if there is no pending i/o,是否有IO操作正在执行*/
ibool buf_pool_check_no_pending_io(void)
{
	ibool	ret;

	mutex_enter(&(buf_pool->mutex));

	if (buf_pool->n_pend_reads + buf_pool->n_flush[BUF_FLUSH_LRU] + buf_pool->n_flush[BUF_FLUSH_LIST]
	+ buf_pool->n_flush[BUF_FLUSH_SINGLE_PAGE] > 0)
		ret = FALSE;
	else
		ret = TRUE;
	}

	mutex_exit(&(buf_pool->mutex));

	return(ret);
}

/*获取空闲的blocks的个数*/
ulint buf_get_free_list_len()
{
	ulint len;
	
	mutex_enter(&(buf_pool->mutex));

	len = UT_LIST_GET_LEN(buf_pool->free);
	
	mutex_exit(&(buf_pool->mutex));

	return len;
}


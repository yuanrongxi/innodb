#include "buf0lru.h"
#include "srv0srv.h"

#include "ut0byte.h"
#include "ut0lst.h"
#include "ut0rnd.h"
#include "sync0sync.h"
#include "sync0rw.h"
#include "hash0hash.h"
#include "os0sync.h"
#include "fil0fil.h"
#include "btr0btr.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "btr0sea.h"
#include "os0file.h"
#include "log0recv.h"

#define BUF_LRU_OLD_TOLERANCE		20
/*LRU的分割比例数*/
#define BUF_LRU_INITIAL_RATIO		8


static void	buf_LRU_block_remove_hashed_page(buf_block_t* block);
static void buf_LRU_block_free_hashed_page(buf_block_t* block);

/*获的最新的block->LRU_position - len / 8*/
ulint buf_LRU_recent_limit()
{
	buf_block_t*	block;
	ulint			len;
	ulint			limit;

	mutex_enter(&(buf_pool->mutex));

	len = UT_LIST_GET_LEN(buf_pool->LRU);
	if(len < BUF_LRU_OLD_MIN_LEN){ /*队列中的单元小于最小old长度，所以不能淘汰任何buf_block*/
		mutex_exit(&(buf_pool->mutex));
		return 0;
	}

	block = UT_LIST_GET_FIRST(buf_pool->LRU);
	limit = block->LRU_position - len / BUF_LRU_INITIAL_RATIO;

	mutex_exit(&(buf_pool->mutex));
}

/*查找是否有可以被置换的buf_block,如果有先free buf_block对应的page*/
ibool buf_LRU_search_and_free_block(ulint n_iterations)
{
	buf_block_t*	block;
	ibool			freed;

	freed = FALSE;
	/*从后面开始查找,因为LRU后面的block是oldest，先淘汰oldest,再考虑淘汰new*/
	block = UT_LIST_GET_LAST(buf_pool->LRU);
	while(block != NULL){
		/*可以被置换出LRU LIST*/
		if(buf_flush_ready_for_replace(block)){
			if (buf_debug_prints)
				printf("Putting space %lu page %lu to free list\n", lock->space, block->offset);

			buf_LRU_block_remove_hashed_page(block);

			mutex_exit(&(buf_pool->mutex));
			/*删除自适应hash索引*/
			btr_search_drop_page_hash_index(block->frame);

			mutex_enter(&(buf_pool->mutex));

			/*必须没有Fix Rule*/
			ut_a(block->buf_fix_count == 0);
			buf_LRU_block_free_hashed_page(block);

			freed = TRUE;
			break;
		}

		block = UT_LIST_GET_PREV(LRU, block);
	}

	/*删除一个完成脏页flush计数*/
	if(buf_pool->LRU_flush_ended > 0)
		buf_pool->LRU_flush_ended--;

	/*假如所有的脏页都没有完成flush disk,那么flush_ended计数器需要设置为0,*/
	if(!freed)
		buf_pool->LRU_flush_ended = 0;

	mutex_exit(&(buf_pool->mutex));

	return freed;
}

/*尝试从LRU list中淘汰一些buf_block*/
void buf_LRU_try_free_flushed_blocks()
{
	mutex_enter(&(buf_pool->mutex));

	while(buf_pool->LRU_flush_ended > 0){
		mutex_exit(&(buf_pool->mutex));
		buf_LRU_search_and_free_block(0);
		mutex_enter(&(buf_pool->mutex));
	}

	mutex_exit(&(buf_pool->mutex));
}

buf_block_t* buf_LRU_get_free_block()
{
	buf_block_t*	block		= NULL;
	ibool		freed;
	ulint		n_iterations	= 0;
	ibool		mon_value_was;
	ibool		started_monitor	= FALSE;

loop:
	mutex_enter(&(buf_pool->mutex));

	/*如果LRU和buffer pool中free的空间和小于buffer pool size的 1/10,说明自适应hash和rec lock占用的空间太多*/
	if(!recv_recovery_on && UT_LIST_GET_LEN(buf_pool->free) + UT_LIST_GET_LEN(buf_pool->LRU) < buf_pool->max_size / 10){
		ut_print_timestamp(stderr);
		fprintf(stderr, "  InnoDB: ERROR: over 9 / 10 of the buffer pool is occupied by\n"
			"InnoDB: lock heaps or the adaptive hash index!\n"
			"InnoDB: We intentionally generate a seg fault to print a stack trace\n"
			"InnoDB: on Linux!\n");

		ut_a(0);
	}
	/*超过80%的buffer pool size被自适应hash和rec lock占用，可能有内存泄露，需要进行内存监控*/
	else if (!recv_recovery_on && UT_LIST_GET_LEN(buf_pool->free) + UT_LIST_GET_LEN(buf_pool->LRU) < buf_pool->max_size / 5){
		ut_print_timestamp(stderr);
		fprintf(stderr, "  InnoDB: WARNING: over 4 / 5 of the buffer pool is occupied by\n"
			"InnoDB: lock heaps or the adaptive hash index! Check that your\n"
			"InnoDB: transactions do not set too many row locks. Starting InnoDB\n"
			"InnoDB: Monitor to print diagnostics, including lock heap and hash index\n"
			"InnoDB: sizes.\n");

		srv_print_innodb_monitor = TRUE;
	}else if (!recv_recovery_on && UT_LIST_GET_LEN(buf_pool->free) + UT_LIST_GET_LEN(buf_pool->LRU) < buf_pool->max_size / 4)
		srv_print_innodb_monitor = FALSE;

	/*有可以置换的buf_block,先将其在从lru队列中删除*/
	if(buf_pool->LRU_flush_ended > 0){
		mutex_exit(&(buf_pool->mutex));
		buf_LRU_try_free_flushed_blocks();
		mutex_exit(&(buf_pool->mutex));
	}
	
	/*buf_pool->free 有buf_block*/
	if(UT_LIST_GET_LEN(buf_pool->free) > 0){
		block = UT_LIST_GET_FIRST(buf_pool->free);
		UT_LIST_REMOVE(free, buf_pool->free, block);
		block->state = BUF_BLOCK_READY_FOR_USE;

		mutex_exit(&(buf_pool->mutex));

		if(started_monitor)
			srv_print_innodb_monitor = mon_value_was;

		return block;
	}

	mutex_exit(&(buf_pool->mutex));

	freed = buf_LRU_search_and_free_block(n_iterations);
	if(freed)
		goto loop;

	if(n_iterations > 30){
		ut_print_timestamp(stderr);
		fprintf(stderr,
			"InnoDB: Warning: difficult to find free blocks from\n"
			"InnoDB: the buffer pool (%lu search iterations)! Consider\n"
			"InnoDB: increasing the buffer pool size.\n",
			n_iterations);

		fprintf(stderr,
			"InnoDB: It is also possible that in your Unix version\n"
			"InnoDB: fsync is very slow, or completely frozen inside\n"
			"InnoDB: the OS kernel. Then upgrading to a newer version\n"
			"InnoDB: of your operating system may help. Look at the\n"
			"InnoDB: number of fsyncs in diagnostic info below.\n");

		fprintf(stderr,
			"InnoDB: Pending flushes (fsync) log: %lu; buffer pool: %lu\n",
			fil_n_pending_log_flushes, fil_n_pending_tablespace_flushes);

		fprintf(stderr,"InnoDB: %lu OS file reads, %lu OS file writes, %lu OS fsyncs\n",
			os_n_file_reads, os_n_file_writes, os_n_fsyncs);

		fprintf(stderr, "InnoDB: Starting InnoDB Monitor to print further\n"
			"InnoDB: diagnostics to the standard output.\n");

		mon_value_was = srv_print_innodb_monitor;
		started_monitor = TRUE;
		srv_print_innodb_monitor = TRUE;
	}

	buf_flush_free_margin();
	/*唤醒所有的IO操作线程*/
	os_aio_simulated_wake_handler_threads();
	if(n_iterations > 10)
		os_thread_sleep(500000);

	n_iterations ++;

	goto loop;
}

/*重新确定old/new list的分界点*/
UNIV_INLINE void buf_LRU_old_adjust_len()
{
	ulint old_len;
	ulint new_len;

	ut_ad(buf_pool->LRU_old);
	ut_ad(mutex_own(&(buf_pool->mutex)));
	ut_ad(3 * (BUF_LRU_OLD_MIN_LEN / 8) > BUF_LRU_OLD_TOLERANCE + 5);

	for(;;){
		old_len = buf_pool->LRU_old_len;
		new_len = 3 * (UT_LIST_GET_LEN(buf_pool->LRU) / 8);

		/*old list太短，需要扩大区域*/
		if(old_len < new_len - BUF_LRU_OLD_TOLERANCE){
			buf_pool->LRU_old = UT_LIST_GET_PREV(LRU, buf_pool->LRU_old);
			(buf_pool->LRU_old)->old = TRUE;
			buf_pool->LRU_old_len++;
		}
		else if(old_len > new_len + BUF_LRU_OLD_TOLERANCE){
			buf_pool->LRU_old->old = FALSE;
			buf_pool->LRU_old = UT_LIST_GET_NEXT(LRU, buf_pool->LRU_old);
			buf_pool->LRU_old_len--;
		}
		else{
			ut_ad(buf_pool->LRU_old);
			return ;
		}
	}
}

static void buf_LRU_old_init()
{
	buf_block_t* block;

	ut_ad(UT_LIST_GET_LEN(buf_pool->LRU) == BUF_LRU_OLD_MIN_LEN);

	/*将LRU中所有的block设置成为old*/
	block = UT_LIST_GET_FIRST(buf_pool->LRU);
	while(block != NULL){
		block->old = TRUE;
		block = UT_LIST_GET_NEXT(LRU, block);
	}

	buf_pool->LRU_old = UT_LIST_GET_FIRST(buf_pool->LRU);
	buf_pool->LRU_old_len = UT_LIST_GET_LEN(buf_pool->LRU);

	/*进行old list和new list分隔点确定*/
	buf_LRU_old_adjust_len();
}

UNIV_INLINE void buf_LRU_remove_block(buf_block_t* block)
{
	ut_ad(buf_pool);
	ut_ad(block);
	ut_ad(mutex_own(&(buf_pool->mutex)));

	/*要删除的BLOCK正好是new old的分界点上*/
	if(block == buf_pool->LRU_old){
		buf_pool->LRU_old = UT_LIST_GET_PREV(LRU, block);
		buf_pool->LRU_old->old = TRUE;

		buf_pool->LRU_old_len ++;
		ut_ad(buf_pool->LRU_old);
	}

	UT_LIST_REMOVE(LRU, buf_pool->LRU, block);
	/*LRU的总长度已经小于old最小能容忍的长度*/
	if(UT_LIST_GET_LEN(buf_pool->LRU) < BUF_LRU_OLD_MIN_LEN){
		buf_pool->LRU_old = NULL;
		return ;
	}

	ut_ad(buf_pool->LRU_old);
	if(block->old) /*如果block是在old list当中，修改old_len*/
		buf_pool->LRU_old_len --;

	buf_LRU_old_adjust_len();
}

/*向LRU list末尾加入一个buf_block*/
UNIV_INLINE void buf_LRU_add_block_to_end_low(buf_block_t* block)
{
	buf_block_t* last_block;

	ut_ad(buf_pool);
	ut_ad(block);
	ut_ad(mutex_own(&(buf_pool->mutex)));

	/*末尾一定是old标示的buf_block*/
	block->old = TRUE;

	last_block = UT_LIST_GET_LAST(buf_pool->LRU);
	if(last_block != NULL)/*确定LRU_position*/
		block->LRU_position = last_block->LRU_position;
	else
		block->LRU_position = buf_pool_clock_tic();

	/*将block加入到LRU的末尾*/
	UT_LIST_ADD_LAST(LRU, buf_pool->LRU, block);
	if(UT_LIST_GET_LEN(buf_pool->LRU) >= BUF_LRU_OLD_MIN_LEN)
		buf_pool->LRU_old_len ++;

	/*进行new和old分隔点的确定*/
	if(UT_LIST_GET_LEN(buf_pool->LRU) > BUF_LRU_OLD_MIN_LEN){ /*长度已经超过了BUF_LRU_OLD_MIN_LEN，重新确定分隔点*/
		ut_ad(buf_pool->LRU_old);
		buf_LRU_old_adjust_len();
	}
	else if(UT_LIST_GET_LEN(buf_pool->LRU) == BUF_LRU_OLD_MIN_LEN) /*刚刚达到old list能容忍的最小长度，可以进行确定new list和old list的分隔点*/
		buf_LRU_old_init();
}

UNIV_INLINE void buf_LRU_add_block_low(buf_block_t* block, ibool old)
{
	ulint	cl;

	ut_ad(buf_pool);
	ut_ad(block);
	ut_ad(mutex_own(&(buf_pool->mutex)));

	/*确定block的位置和类型*/
	block->old = old;
	cl = buf_pool_clock_tic();

	/*如果block是加入到new list或者LRU的长度不超过BUF_LRU_OLD_MIN_LEN,那么block插入到lru 第一个位置*/
	if(!old || (UT_LIST_GET_LEN(buf_pool->LRU) < BUF_LRU_OLD_MIN_LEN)){
		UT_LIST_ADD_FIRST(LRU, buf_pool->LRU, block);

		block->LRU_position = cl;		
		block->freed_page_clock = buf_pool->freed_page_clock;
	}
	else{ /*如果是old，那么将block插入到LRU_old_block的后面，也就是分隔点的后面*/
		UT_LIST_INSERT_AFTER(LRU, buf_pool->LRU, buf_pool->LRU_old, block);
		buf_pool->LRU_old_len++;

		block->LRU_position = buf_pool->LRU_old->LRU_position;
	}

	/*重新确定分隔点*/
	if(UT_LIST_GET_LEN(buf_pool->LRU) > BUF_LRU_OLD_MIN_LEN){
		ut_ad(buf_pool->LRU_old);
		buf_LRU_old_adjust_len();
	}
	else if(UT_LIST_GET_LEN(buf_pool->LRU) == BUF_LRU_OLD_MIN_LEN)
		buf_LRU_old_init();
}

void buf_LRU_add_block(buf_block_t* block, ibool old)
{
	buf_LRU_add_block_low(block, old);
}

/*将buf_block放入new队列中*/
void buf_LRU_make_block_young(buf_block_t* block)
{
	buf_LRU_remove_block(block);
	buf_LRU_add_block_low(block, FALSE);
}

/*将buf_block放入old队列中*/
void buf_LRU_make_block_old(buf_block_t* block)
{
	buf_LRU_remove_block(block);
	buf_LRU_add_block_to_end_low(block);
}

/*将buf_block加入到buf_pool的free队列中*/
void buf_LRU_block_free_non_file_page(buf_block_t* block)
{
	ut_ad(mutex_own(&(buf_pool->mutex)));
	ut_ad(block);

	ut_ad((block->state == BUF_BLOCK_MEMORY) || (block->state == BUF_BLOCK_READY_FOR_USE));

	UT_LIST_ADD_FIRST(free, buf_pool->free, block);
}

/*将block从LRU中删除，并且解除buf_block与（space id, page_no）的哈希对应关系*/
static void buf_LRU_block_remove_hashed_page(buf_block_t* block)
{
	ut_ad(mutex_own(&(buf_pool->mutex)));
	ut_ad(block);

	ut_ad(block->state == BUF_BLOCK_FILE_PAGE);

	ut_a(block->io_fix == 0);
	ut_a(block->buf_fix_count == 0);
	ut_a(ut_dulint_cmp(block->oldest_modification, ut_dulint_zero) == 0);

	/*将buf_block从LRU中删除*/
	buf_LRU_remove_block(block);

	buf_pool->freed_page_clock ++;
	/*block->modify_clock自加*/
	buf_frame_modify_clock_inc(block->frame);

	/*删除(space id, page_no)与buf_block的hash表对应关系*/
	HASH_DELETE(buf_block_t, hash, buf_pool->page_hash, buf_page_address_fold(block->space, block->offset), block);

	block->state = BUF_BLOCK_REMOVE_HASH;
}

/*将block返还给free list*/
static void buf_LRU_block_free_hashed_page(buf_block_t* block)
{
	ut_ad(mutex_own(&(buf_pool->mutex)));
	ut_ad(block->state == BUF_BLOCK_REMOVE_HASH);

	block->state = BUF_BLOCK_MEMORY;

	buf_LRU_block_free_non_file_page(block);
}

/*检查LRU list的合法性*/
ibool buf_LRU_validate(void)
{
	buf_block_t*	block;
	ulint		old_len;
	ulint		new_len;
	ulint		LRU_pos;
	
	ut_ad(buf_pool);
	mutex_enter(&(buf_pool->mutex));

	if (UT_LIST_GET_LEN(buf_pool->LRU) >= BUF_LRU_OLD_MIN_LEN) {

		ut_a(buf_pool->LRU_old);
		old_len = buf_pool->LRU_old_len;
		new_len = 3 * (UT_LIST_GET_LEN(buf_pool->LRU) / 8);
		ut_a(old_len >= new_len - BUF_LRU_OLD_TOLERANCE);
		ut_a(old_len <= new_len + BUF_LRU_OLD_TOLERANCE);
	}
		
	UT_LIST_VALIDATE(LRU, buf_block_t, buf_pool->LRU);

	block = UT_LIST_GET_FIRST(buf_pool->LRU);

	old_len = 0;

	while (block != NULL) {
		ut_a(block->state == BUF_BLOCK_FILE_PAGE);

		if (block->old)
			old_len++;

		if (buf_pool->LRU_old && (old_len == 1))
			ut_a(buf_pool->LRU_old == block);

		LRU_pos	= block->LRU_position;

		block = UT_LIST_GET_NEXT(LRU, block);

		if (block) {
			/* If the following assert fails, it may not be an error: just the buf_pool clockhas wrapped around */
			ut_a(LRU_pos >= block->LRU_position);
		}
	}

	if (buf_pool->LRU_old)
		ut_a(buf_pool->LRU_old_len == old_len);

	UT_LIST_VALIDATE(free, buf_block_t, buf_pool->free);

	block = UT_LIST_GET_FIRST(buf_pool->free);

	while (block != NULL) {
		ut_a(block->state == BUF_BLOCK_NOT_USED);
		block = UT_LIST_GET_NEXT(free, block);
	}

	mutex_exit(&(buf_pool->mutex));

	return(TRUE);
}

/*对LRU中的信息进行打印*/
void buf_LRU_print(void)
{
	buf_block_t*	block;
	buf_frame_t*	frame;
	ulint		len;

	ut_ad(buf_pool);
	mutex_enter(&(buf_pool->mutex));

	printf("Pool ulint clock %lu\n", buf_pool->ulint_clock);

	block = UT_LIST_GET_FIRST(buf_pool->LRU);

	len = 0;

	while (block != NULL) {
		printf("BLOCK %lu ", block->offset);

		if (block->old)
			printf("old ");

		if (block->buf_fix_count)
			printf("buffix count %lu ", block->buf_fix_count);

		if (block->io_fix)
			printf("io_fix %lu ", block->io_fix);

		if (ut_dulint_cmp(block->oldest_modification, ut_dulint_zero) > 0)
				printf("modif. ");

		printf("LRU pos %lu ", block->LRU_position);

		frame = buf_block_get_frame(block);

		printf("type %lu ", fil_page_get_type(frame));
		printf("index id %lu ", ut_dulint_get_low(btr_page_get_index_id(frame)));

		block = UT_LIST_GET_NEXT(LRU, block);
		len++;
		if (len % 10 == 0)
			printf("\n");
	}

	mutex_exit(&(buf_pool->mutex));
}


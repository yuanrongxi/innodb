#include "log0log.h"

#include "mem0mem.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "srv0srv.h"
#include "log0recv.h"
#include "fil0fil.h"
#include "dict0boot.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "trx0sys.h"
#include "trx0trx.h"

#include <stdint.h>

/*当前free的限制，0表示没有进行初始化*/
ulint log_fsp_current_free_limit = 0;
/*全局log对象*/
log_t*	log_sys	= NULL;

ibool log_do_write = TRUE;
ibool log_debug_writes = FALSE;

byte log_archive_io;

/*2K*/
#define LOG_BUF_WRITE_MARGIN	(4 * OS_FILE_LOG_BLOCK_SIZE)

#define LOG_BUF_FLUSH_RATIO		2
/*2K + 64K*/
#define LOG_BUF_FLUSH_MARGIN	(LOG_BUF_WRITE_MARGIN + 4 * UNIV_PAGE_SIZE)
/*64K*/
#define LOG_CHECKPOINT_FREE_PER_THREAD	(4 * UNIV_PAGE_SIZE)
/*128K*/
#define LOG_CHECKPOINT_EXTRA_FREE		(8 * UNIV_PAGE_SIZE)

/*异步的checkpoint,控制生成一个checkpoint比例*/
#define LOG_POOL_CHECKPOINT_RATIO_ASYNC 32
/*同步flush比例*/
#define LOG_POOL_PREFLUSH_RATIO_SYNC	16
/*异步的flush比例*/
#define LOG_POOL_PREFLUSH_RATIO_ASYNC	8

#define LOG_ARCHIVE_EXTRA_MARGIN		(4 * UNIV_PAGE_SIZE)

#define LOG_ARCHIVE_RATIO_ASYNC			16

#define LOG_UNLOCK_NONE_FLUSHED_LOCK	1
#define LOG_UNLOCK_FLUSH_LOCK			2

/*Archive的操作类型*/
#define	LOG_ARCHIVE_READ				1
#define	LOG_ARCHIVE_WRITE				2


/*完成checkpoint的写io操作*/
static void log_io_complete_checkpoint(log_group_t* group);
/*完成archive的io操作*/
static void log_io_complete_archive();
/*检查archive日志文件归档是否可以触发*/
static void log_archive_margin();

/*设置fsp_current_free_limit,这个改变有可能会产生一个checkpoint*/
void log_fsp_current_free_limit_set_and_checkpoint(ulint limit)
{
	ibool success;
	mutex_enter(&(log_sys->mutex));
	log_fsp_current_free_limit = limit;
	mutex_exit(&(log_sys->mutex));

	success = FALSE;
	while(!success){
		success = log_checkpoint(TRUE, TRUE);
	}
}
/*获得buf pool当中最老的lsn，如果buf pool中的oldest = 0，默认返回log_sys中的lsn*/
static dulint log_buf_pool_get_oldest_modification()
{
	dulint lsn;

	ut_ad(mutex_own(&(log_sys->mutex)));
	/*buf_pool_get_oldest_modification是buf0buf中，返回buf pool中修改的block当中最旧的lsn*/
	lsn = buf_pool_get_oldest_modification();
	if(ut_dulint_is_zero(lsn))
		lsn = log_sys->lsn;

	return lsn;
}

/*打开一个新的block，在打开前，需要判断log->buf的剩余空间和归档的buf空间*/
dulint log_reserve_and_open(ulint len)
{
	log_t*	log	= log_sys;
	ulint	len_upper_limit;
	ulint	archived_lsn_age;
	ulint	count = 0;
	ulint	dummy;

	ut_a(len < log->buf_size / 2);
loop:
	mutex_enter(&(log->mutex));

	/*计算长度上限*/
	len_upper_limit = LOG_BUF_FLUSH_MARGIN + (5 * len) / 4;
	if(log->buf_free + len_upper_limit > log->buf_size){/*长度超过了buf_size,需要对log buffer进行flush_up*/
		mutex_exit(&(log->mutex));

		/*没有足够的空间，同步将log buffer刷入磁盘*/
		log_flush_up_to(ut_dulint_max, LOG_WAIT_ALL_GROUPS);

		count ++;
		ut_ad(count < 50);
		goto loop;
	}

	/*log归档选项是激活的*/
	if(log->archiving_state != LOG_ARCH_OFF){
		/*计算lsn和archived_lsn的差值*/
		archived_lsn_age = ut_dulint_minus(log->lsn, log->archived_lsn);
		if(archived_lsn_age + len_upper_limit > log->max_archived_lsn_age){ /*正在存档状态又超过存档的lsn 最大范围*/
			mutex_exit(&(log->mutex));

			ut_ad(len_upper_limit <= log->max_archived_lsn_age);
			/*强制同步进行archive write*/
			log_archive_do(TRUE, &dummy);
			cout ++;
			
			ut_ad(count < 50);

			goto loop;
		}
	}

#ifdef UNIV_LOG_DEBUG
	log->old_buf_free = log->buf_free;
	log->old_lsn = log->lsn;
#endif	

	return log->lsn;
}

void log_write_low(byte* str, ulint str_len)
{
	log_t*	log	= log_sys;
	ulint	len;
	ulint	data_len;
	byte*	log_block;

	ut_ad(mutex_own(&(log->mutex)));

part_loop:
	/*计算part length*/
	data_len = log->buf_free % OS_FILE_LOG_BLOCK_SIZE + str_len;
	if(data_len < OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) /*可以在同一个block当中*/
		len = str_len;
	else{
		data_len = OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
		/*将block相对相对剩余的长度作为len*/
		len = OS_FILE_LOG_BLOCK_SIZE - (log->buf_free % OS_FILE_LOG_BLOCK_SIZE) - LOG_BLOCK_TRL_SIZE;
	}
	/*将日志内容拷贝到log buffer*/
	ut_memcpy(log->buf + log->buf_free, str, len);
	str_len -= len;
	str = str + len;

	log_block = ut_align(log->buf + log->buf_free, OS_FILE_LOG_BLOCK_SIZE);
	log_block_set_data_len(log_block, data_len);

	if(data_len = OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE){ /*完成一个block的写入*/
		log_block_set_data_len(log_block, OS_FILE_LOG_BLOCK_SIZE); /*重新设置长度*/
		log_block_set_checkpoint_no(log_block, log_sys->next_checkpoint_no); /*设置checkpoint number*/

		len += LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE;
		log->lsn = ut_dulint_add(log->lsn, len);
		/*初始化一个新的block*/
		log_block_init(log_block + OS_FILE_LOG_BLOCK_SIZE, log->lsn);
	}
	else /*更改lsn*/
		log->lsn = ut_dulint_add(log->lsn, len);

	log->buf_free += len;
	ut_ad(log->buf_free <= log->buf_size);

	if(str_len > 0)
		goto part_loop;
}
/*在mtr_commit事务提交的时候调用*/
dulint log_close()
{
	byte*	log_block;
	ulint	first_rec_group;
	dulint	oldest_lsn;
	dulint	lsn;
	log_t*	log	= log_sys;

	ut_ad(mutex_own(&(log->mutex)));

	lsn = log->lsn;

	/*如果这block的first_rec_group = 0说明first_rec_group没有被设置，设置block的数据长度为其first_rec_group*/
	log_block = ut_align_down(log->buf + log->buf_free, OS_FILE_LOG_BLOCK_SIZE);
	first_rec_group = log_block_get_first_rec_group(log_block);
	if(first_rec_group == 0)
		log_block_set_first_rec_group(log_block, log_block_get_data_len(log_block));


	/*超过最大的buf free,需要进行flush或者建立checkpoint*/
	if(log->buf_free > log->max_buf_free)
		log->check_flush_or_checkpoint = TRUE;

	/*如果lsn差值还没达到检查点容忍的差值，直接退出函数*/
	if(ut_dulint_minus(lsn, log->last_checkpoint_lsn) <= log->max_modified_age_async)
		goto function_exit;

	oldest_lsn = buf_pool_get_oldest_modification();
	/*对lsn的判断，如果oldest_lsn与当前lsn的差值同步的阈值就启动flush或者建立一个checkpoint*/
	if(ut_dulint_is_zero(oldest_lsn) || (ut_dulint_minus(lsn, oldest_lsn) > log->max_modified_age_async)
		|| (ut_dulint_minus(lsn, log->last_checkpoint_lsn) > log->max_checkpoint_age_async))
			log->check_flush_or_checkpoint = TRUE;

function_exit:
#ifdef UNIV_LOG_DEBUG
	log_check_log_recs(log->buf + log->old_buf_free, log->buf_free - log->old_buf_free, log->old_lsn);
#endif

	return lsn;
}

/*在归档前，填充最后一个block*/
static void log_pad_current_log_block()
{
	byte	b = MLOG_DUMMY_RECORD;
	ulint	pad_length;
	ulint	i;
	dulint	lsn;

	/*尝试开启一个新的block*/
	lsn = log_reserve_and_open(OS_FILE_LOG_BLOCK_SIZE);
	/*计算可填充的长度并用b进行数据填充*/
	pad_length = OS_FILE_LOG_BLOCK_SIZE - (log_sys->buf_free % OS_FILE_LOG_BLOCK_SIZE) - LOG_BLOCK_TRL_SIZE;
	for(i = 0; i < pad_length; i++)
		log_write_low(&b, 1);

	lsn = log_sys->lsn;
	log_close();
	log_release();

	ut_a((ut_dulint_get_low(lsn) % OS_FILE_LOG_BLOCK_SIZE) == LOG_BLOCK_HDR_SIZE);
}

/*获得一个group 可以容纳的日志长度,一个group有相同长度的文件组成*/
ulint log_group_get_capacity(log_group_t* group)
{
	ut_ad(mutex_own(&(log_sys->mutex)));
	return((group->file_size - LOG_FILE_HDR_SIZE) * group->n_files); 
}

/*将group 内部的相对偏移换算绝对偏移换，绝对偏移量 = offset - 文件头长度 */
UNIV_INLINE ulint log_group_calc_size_offset(ulint offset, log_group_t* group)
{
	ut_ad(mutex_own(&(log_sys->mutex)));
	return offset - LOG_FILE_HDR_SIZE * (1 + offset / group->file_size);
}

/*通过绝对偏移获得在group file里的相对偏移量*/
UNIV_INLINE ulint log_group_calc_real_offset(ulint offset, log_group_t* group)
{
	ut_ad(mutex_own(&(log_sys->mutex)));

	return (offset + LOG_FILE_HDR_SIZE * (1 + offset / (group->file_size - LOG_FILE_HDR_SIZE)));
}

/*返回lsn相对group起始位置的相对位置*/
static ulint log_group_calc_lsn_offset(dulint lsn, log_group_t* group)
{
	dulint	        gr_lsn;
	int64_t			gr_lsn_size_offset;
	int64_t			difference;
	int64_t			group_size;
	int64_t			offset;

	ut_ad(mutex_own(&(log_sys->mutex)));

	gr_lsn = group->lsn;
	/*获得lsn_offset绝对偏移,去除文件头长度*/
	gr_lsn_size_offset = (int64_t)(log_group_calc_size_offset(group->lsn_offset, group));
	/*获得group的容量*/
	group_size = log_group_get_capacity(group);
	/*计算grl_lsn和lsn之间的差值绝对值*/
	if(ut_dulint_cmp(lsn, gr_lsn) >= 0){
		difference = (int64_t) ut_dulint_minus(lsn, gr_lsn);
	}
	else{ 
		difference = (int64_t)ut_dulint_minus(gr_lsn, lsn);
		difference = difference % group_size;
		difference = group_size - difference;
	}

	/*获得相对group size的偏移量*/
	offset = (gr_lsn_size_offset + difference) % group_size;
	ut_a(offset <= 0xFFFFFFFF);

	/*返回相对的位置,加上文件头长度*/
	return log_group_calc_real_offset(offset, group);
}

/*获得lsn对应group的文件编号和在文件中对相对起始位置*/
ulint log_calc_where_lsn_is(int64_t* log_file_offset, dulint first_header_lsn, dulint lsn, ulint n_log_files, int64_t log_file_size)
{
	int64_t	ib_lsn;
	int64_t	ib_first_header_lsn;
	int64_t	capacity = log_file_size - LOG_FILE_HDR_SIZE; /*单个group文件可以容纳的数据长度*/
	ulint	file_no;
	int64_t	add_this_many;

	ib_lsn = ut_conv_dulint_to_longlong(lsn);
	ib_first_header_lsn = ut_conv_dulint_to_longlong(first_header_lsn);

	if(ib_lsn < ib_first_header_lsn){
		add_this_many = 1 + (ib_first_header_lsn - ib_lsn) / (capacity * (int64_t)n_log_files);
		ib_lsn += add_this_many * capacity * (int64_t)n_log_files;
	}

	ut_a(ib_lsn >= ib_first_header_lsn);

	file_no = ((ulint)((ib_lsn - ib_first_header_lsn) / capacity)) % n_log_files; 	/*获得文件的序号*/

	/*计算除去group header的位置*/
	*log_file_offset = (ib_lsn - ib_first_header_lsn) % capacity;
	/*计算相对于文件其实位置的位置*/
	*log_file_offset = *log_file_offset + LOG_FILE_HDR_SIZE;

	return file_no;
}

void log_group_set_fields(log_group_t* group, dulint lsn)
{
	/*获得最新的相对偏移量,加上了LOG_FILE_HDR_SIZE的位置*/
	group->lsn_offset = log_group_calc_lsn_offset(lsn, group);
	/*设置新的lsn值*/
	group->lsn = lsn;
}

/*计算日志刷盘、建立checkpoint和归档的触发阈值*/
static ibool log_calc_max_ages()
{
	log_group_t*	group;
	ulint		n_threads;
	ulint		margin;
	ulint		free;
	ulint		smallest_capacity;	
	ulint		archive_margin;
	ulint		smallest_archive_margin;
	ibool		success		= TRUE;

	ut_ad(!mutex_own(&(log_sys->mutex)));

	/*获得服务的线程数*/
	n_threads = srv_get_n_threads();
	
	mutex_enter(&(log_sys->mutex));
	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	
	ut_ad(group);
	smallest_capacity = ULINT_MAX;
	smallest_archive_margin = ULINT_MAX;

	/*重新计算smallest_capacity和smallest_archive_margin*/
	while(group){
		/*重新评估最小的group能容纳的数据*/
		if(log_group_get_capacity(group) < smallest_capacity)
			smallest_capacity = log_group_get_capacity(group);
		 /*归档的margin是group的容量 减去一个group file的大小 再预留64K*/
		archive_margin = log_group_get_capacity(group) - (group->file_size - LOG_FILE_HDR_SIZE) - LOG_ARCHIVE_EXTRA_MARGIN;
		if(archive_margin < smallest_archive_margin)
			smallest_archive_margin = archive_margin;

		group = UT_LIST_GET_NEXT(log_groups, group);
	}

	/*为每个线程预留64K的空间*/
	free = LOG_CHECKPOINT_FREE_PER_THREAD * n_threads + LOG_CHECKPOINT_EXTRA_FREE;
	if(free >= smallest_capacity / 2){
		success = FALSE;
		goto failure;
	}
	else
		margin = smallest_capacity - free;

	margin = ut_min(margin, log_sys->adm_checkpoint_interval);

	/*计算日志写盘的阈值*/
	log_sys->max_modified_age_async = margin - margin / LOG_POOL_PREFLUSH_RATIO_ASYNC;		/*modified剩余1/8作为异步阈值*/
	log_sys->max_modified_age_sync = margin - margin / LOG_POOL_PREFLUSH_RATIO_SYNC;		/*modified剩余1/16作为同步阈值*/
	log_sys->max_checkpoint_age_async = margin - margin / LOG_POOL_CHECKPOINT_RATIO_ASYNC;	/*checkpoint剩余1/32作为异步阈值*/
	log_sys->max_checkpoint_age = margin;	/*无任何剩余作为强制同步checkpoint阈值*/
	log_sys->max_archived_lsn_age = smallest_archive_margin;
	log_sys->max_archived_lsn_age_async = smallest_archive_margin - smallest_archive_margin / LOG_ARCHIVE_RATIO_ASYNC;

failure:
	mutex_exit(&(log_sys->mutex));
	if(!success)
		fprintf(stderr, "Error: log file group too small for the number of threads\n");

	return success;
}

/*初始化log_sys,在引擎初始化的时候调用*/
void log_init()
{
	byte* buf;

	log_sys = mem_alloc(sizeof(log_t));

	/*建立latch对象*/
	mutex_create(&(log_sys->mutex));
	mutex_set_level(&(log_sys->mutex), SYNC_LOG);

	mutex_enter(&(log_sys->mutex));
	log_sys->lsn = LOG_START_LSN;
	ut_a(LOG_BUFFER_SIZE >= 16 * OS_FILE_LOG_BLOCK_SIZE);
	ut_a(LOG_BUFFER_SIZE >= 4 * UNIV_PAGE_SIZE);

	/*为了512字节对齐，所以在开辟内存的时候一定要大于512,buf的长度为log_buff_size个page的长度*/
	buf = ut_malloc(LOG_BUFFER_SIZE + OS_FILE_LOG_BLOCK_SIZE);

	/*512字节对齐*/
	log_sys->buf = ut_align(buf, OS_FILE_LOG_BLOCK_SIZE);
	log_sys->buf_size = LOG_BUFFER_SIZE;
	memset(log_sys->buf, 0, LOG_BUFFER_SIZE);

	log_sys->max_buf_free = log_sys->buf_size / LOG_BUF_FLUSH_RATIO - LOG_BUF_FLUSH_MARGIN;
	log_sys->check_flush_or_checkpoint = TRUE;

	UT_LIST_INIT(log_sys->log_groups);
	log_sys->n_log_ios = 0;

	log_sys->n_log_ios_old = log_sys->n_log_ios;
	log_sys->last_printout_time = time(NULL);
	log_sys->buf_next_to_write = 0;
	/*flush lsn设置为0*/
	log_sys->flush_lsn = ut_dulint_zero;
	log_sys->written_to_some_lsn = log_sys->lsn;
	log_sys->written_to_all_lsn = log_sys->lsn;

	log_sys->n_pending_writes = 0;
	
	log_sys->no_flush_event = os_event_create(NULL);
	os_event_set(log_sys->no_flush_event);

	log_sys->one_flushed_event = os_event_create(NULL);
	os_event_set(log_sys->one_flushed_event);

	log_sys->adm_checkpoint_interval = ULINT_MAX;
	log_sys->next_checkpoint_no = ut_dulint_zero;
	log_sys->last_checkpoint_lsn = log_sys->lsn;
	log_sys->n_pending_checkpoint_writes = 0; 

	rw_lock_create(&(log_sys->checkpoint_lock));
	rw_lock_set_level(&(log_sys->checkpoint_lock), SYNC_NO_ORDER_CHECK);

	/*存储checkpoint信息的buf, 一个OS_FILE_LOG_BLOCK_SIZE长度即可*/
	log_sys->checkpoint_buf = ut_align(mem_alloc(2 * OS_FILE_LOG_BLOCK_SIZE), OS_FILE_LOG_BLOCK_SIZE);
	memset(log_sys->checkpoint_buf, 0, OS_FILE_LOG_BLOCK_SIZE);

	log_sys->archiving_state = LOG_ARCH_ON;
	log_sys->archived_lsn = log_sys->lsn;
	log_sys->next_archived_lsn = ut_dulint_zero;

	log_sys->n_pending_archive_ios = 0;

	rw_lock_create(&(log_sys->archive_lock));
	rw_lock_set_level(&(log_sys->archive_lock), SYNC_NO_ORDER_CHECK);

	/*构建archive buf*/
	log_sys->archive_buf = ut_align(ut_malloc(LOG_ARCHIVE_BUF_SIZE + OS_FILE_LOG_BLOCK_SIZE), OS_FILE_LOG_BLOCK_SIZE);
	log_sys->archive_buf_size = LOG_ARCHIVE_BUF_SIZE;
	memset(log_sys->archive_buf, '\0', LOG_ARCHIVE_BUF_SIZE);

	log_sys->archiving_on = os_event_create(NULL);
	log_sys->online_backup_state = FALSE;

	/*初始化第一个block*/
	log_block_init(log_sys->buf, log_sys->lsn);
	log_block_set_first_rec_group(log_sys->buf, LOG_BLOCK_HDR_SIZE);

	/*设置初始化偏移量和其实位置*/
	log_sys->buf_free = LOG_BLOCK_HDR_SIZE;
	log_sys->lsn = ut_dulint_add(LOG_START_LSN, LOG_BLOCK_HDR_SIZE);

	mutex_exit(&(log_sys->mutex));

#ifdef UNIV_LOG_DEBUG
	recv_sys_create();
	recv_sys_init(FALSE, buf_pool_get_curr_size());

	recv_sys->parse_start_lsn = log_sys->lsn;
	recv_sys->scanned_lsn = log_sys->lsn;
	recv_sys->scanned_checkpoint_no = 0;
	recv_sys->recovered_lsn = log_sys->lsn;
	recv_sys->limit_lsn = ut_dulint_max;
#endif
}

/*在数据库创创建的时候调用*/
void log_group_init(ulint id, ulint n_files, ulint file_size, ulint space_id, ulint archive_space_id)
{
	ulint	i;
	log_group_t* group;

	group = mem_alloc(sizeof(log_group_t));
	group->id = id;
	group->n_files = n_files;
	group->file_size = file_size;
	group->space_id = space_id;	/*保存重做日志的fil_space*/
	group->state = LOG_GROUP_OK;
	group->lsn = LOG_START_LSN;
	group->lsn_offset = LOG_FILE_HDR_SIZE;
	group->n_pending_writes = 0;

	group->file_header_bufs = mem_alloc(sizeof(byte*) * n_files);
	group->archive_file_header_bufs = mem_alloc(sizeof(byte*) * n_files);

	for(i = 0; i < n_files; i ++){
		*(group->file_header_bufs + i) = ut_align(mem_alloc(LOG_FILE_HDR_SIZE + OS_FILE_LOG_BLOCK_SIZE), OS_FILE_LOG_BLOCK_SIZE);
		memset(*(group->file_header_bufs + i), 0, LOG_FILE_HDR_SIZE);

		*(group->archive_file_header_bufs + i) = ut_align(mem_alloc(LOG_FILE_HDR_SIZE + OS_FILE_LOG_BLOCK_SIZE), OS_FILE_LOG_BLOCK_SIZE);
		memset(*(group->archive_file_header_bufs + i), 0, LOG_FILE_HDR_SIZE);
	}

	group->archive_space_id = archive_space_id; /*保存归档日志的fil_space*/
	group->archived_file_no = 0;
	group->archived_offset = 0;

	group->checkpoint_buf = ut_align(mem_alloc(2 * OS_FILE_LOG_BLOCK_SIZE), OS_FILE_LOG_BLOCK_SIZE);
	memset(group->checkpoint_buf, 0, OS_FILE_LOG_BLOCK_SIZE);

	UT_LIST_ADD_LAST(log_groups, log_sys->log_groups, group);

	ut_a(log_calc_max_ages());
}

/*触发unlock信号在io flush完成之后*/
UNIV_INLINE void log_flush_do_unlocks(ulint code)
{
	ut_ad(mutex_own(&(log_sys->mutex)));

	/*触发one_flushed_event*/
	if(code & LOG_UNLOCK_NONE_FLUSHED_LOCK)
		os_event_set(log_sys->one_flushed_event);

	if(code & LOG_UNLOCK_FLUSH_LOCK)
		os_event_set(log_sys->no_flush_event);
}

/*检查group 的io flush是否完成,更改将flush_lsn设置为written_to_some_lsn*/
UNIV_INLINE ulint log_group_check_flush_completion(log_group_t* group)
{
	ut_ad(mutex_own(&(log_sys->mutex)));
	
	/*本group已经没有IO操作在进行*/
	if(!log_sys->one_flushed && group->n_pending_writes == 0){
		if(log_debug_writes)
			printf("Log flushed first to group %lu\n", group->id);

		log_sys->written_to_some_lsn = log_sys->flush_lsn;
		log_sys->one_flushed = TRUE;

		return LOG_UNLOCK_NONE_FLUSHED_LOCK;
	}

	if(log_debug_writes && group->n_pending_writes == 0)
		printf("Log flushed to group %lu\n", group->id);

	return 0;
}

static ulint log_sys_check_flush_completion()
{
	ulint	move_start;
	ulint	move_end;

	ut_ad(mutex_own(&(log_sys->mutex)));
	/*整个log_sys没有io操作在进行*/
	if(log_sys->n_pending_writes == 0){
		log_sys->written_to_all_lsn = log_sys->flush_lsn;
		log_sys->buf_next_to_write = log_sys->flush_end_offset; /*下次进行log flush操作数据起始偏移*/

		/*数据向前移，因为全面的数据已经flush到磁盘*/
		if(log_sys->flush_end_offset > log_sys->max_buf_free / 2){
			/*确定移动的位置*/
			move_start = ut_calc_align_down(log_sys->flush_end_offset, OS_FILE_LOG_BLOCK_SIZE);
			move_end = ut_calc_align(log_sys->buf_free, OS_FILE_LOG_BLOCK_SIZE);

			ut_memmove(log_sys->buf, log_sys->buf + move_start, move_end - move_start);
			/*重新设置buf_free和buf_next_to_write*/
			log_sys->buf_free -= move_start;
			log_sys->buf_next_to_write -= move_start;
		}

		return(LOG_UNLOCK_FLUSH_LOCK);
	}

	return 0;
}

/*完成group的io操作*/
void log_io_complete(log_group_t* group)
{
	ulint unlock;
	/*一个日志归档类完成io操作,归档IO操作会将log_archive_io传入aio控制模块*/
	if((byte*)group == &log_archive_io){
		log_io_complete_archive();
		return;
	}

	/*一个checkpoint IO,在checkpoint fil_io传入的group指针加了1，这样做的目的应该是区分checkpoint io和日志文件IO*/
	if((ulint)group & 0x1){
		group = (log_group_t*)((ulint)group - 1);
		/*将file0file中的space写入到硬盘*/
		if (srv_unix_file_flush_method != SRV_UNIX_O_DSYNC && srv_unix_file_flush_method != SRV_UNIX_NOSYNC)
			fil_flush(group->space_id);

		log_io_complete_checkpoint(group);

		return;
	}

	ut_a(0); /*innodb都是采用同步写的方式写日志，一般不会到下面的代码中*/
	
	if(srv_unix_file_flush_method != SRV_UNIX_O_DSYNC && srv_unix_file_flush_method != SRV_UNIX_NOSYNC && srv_flush_log_at_trx_commit != 2)
	{
		fil_flush(group->space_id);
	}

	mutex_enter(&(log_sys->mutex));

	ut_a(group->n_pending_writes > 0);
	ut_a(log_sys->n_pending_writes > 0);

	group->n_pending_writes--;
	log_sys->n_pending_writes--;
	/*对单个group的状态做更新*/
	unlock = log_group_check_flush_completion(group);
	/*对sys_log状态做更新*/
	unlock = unlock | log_sys_check_flush_completion();
	/*触发io flush完成的信号*/
	log_flush_do_unlocks(unlock);

	mutex_exit(&(log_sys->mutex));
}

/*在Master thread中每秒调用一次,主要功能是对日志进行flush*/
void log_flush_to_disk()
{
	log_group_t* group;

loop:
	mutex_enter(&(log_sys->mutex));
	/*有fil_flush正在执行,等待所有正在进行的fil_flush结束*/
	if(log_sys->n_pending_writes > 0){
		mutex_exit(&(log_sys->mutex));
		/*进入等待状态*/
		os_event_wait(log_sys->no_flush_event);
		
		goto loop;
	}

	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	/*进行space刷盘时，防止其他线程同时进行*/
	log_sys->n_pending_writes ++;
	group->n_pending_writes ++;

	/*让其他刷盘行为等待，例如：checkpoint*/
	os_event_reset(log_sys->no_flush_event);
	os_event_reset(log_sys->one_flushed_event);

	mutex_exit(&(log_sys->mutex));

	/*log文件刷盘*/
	fil_flush(group->space_id);

	mutex_enter(&(log_sys->mutex));
	ut_a(group->n_pending_writes == 1);
	ut_a(log_sys->n_pending_writes == 1);

	/*刷盘完成，pending回到完成的状态，以便log_io_complete进行判断*/
	group->n_pending_writes--;
	log_sys->n_pending_writes--;

	os_event_set(log_sys->no_flush_event);
	os_event_set(log_sys->one_flushed_event);

	mutex_exit(&(log_sys->mutex));
}

/*将group header写入到log file的page cache当中*/
static void log_group_file_header_flush(ulint type, log_group_t* group, ulint nth_file, dulint start_lsn)
{
	byte*	buf;
	ulint	dest_offset;

	ut_ad(mutex_own(&(log_sys->mutex)));
	ut_a(nth_file < group->n_files);

	/*找到文件对应goup中的头缓冲区*/
	buf = *(group->file_header_bufs + nth_file);

	/*写入group id*/
	mach_write_to_4(buf + LOG_GROUP_ID, group->id);
	/*写入起始的lsn*/
	mach_write_to_8(buf + LOG_FILE_START_LSN, start_lsn);
	/*第12 ~ 第16个字节是存储file no*/
	memcpy(buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "    ", 4);

	dest_offset = nth_file * group->file_size;
	if(log_debug_writes)
		printf("Writing log file header to group %lu file %lu\n", group->id, nth_file);

	if(log_do_write){
		log_sys->n_log_ios++;

		/*调用异步io进行文件写入,同步写入*/
		fil_io(OS_FILE_WRITE | OS_FILE_LOG, TRUE, group->space_id, dest_offset / UNIV_PAGE_SIZE, dest_offset % UNIV_PAGE_SIZE, OS_FILE_LOG_BLOCK_SIZE,
			buf, group);
	}
}
/*设置block的check sum*/
static void log_block_store_checksum(byte* block)
{
	log_block_set_checksum(block, log_block_calc_checksum(block));
}

void log_group_write_buf(ulint type, log_group_t* group, byte* buf, ulint len, dulint start_lsn, ulint new_data_offset)
{
	ulint	write_len;
	ibool	write_header;
	ulint	next_offset;
	ulint	i;

	ut_ad(mutex_own(&(log_sys->mutex)));
	ut_a(len % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_a(ut_dulint_get_low(start_lsn) % OS_FILE_LOG_BLOCK_SIZE == 0);

	if(new_data_offset == 0)
		write_header = TRUE;
	else
		write_header = FALSE;

loop:
	if(len == 0)
		return ;

	next_offset = log_group_calc_lsn_offset(start_lsn, group);
	if((next_offset % group->file_size == LOG_FILE_HDR_SIZE) && write_header){ /*可以进行group header头信息写log file*/
		log_group_file_header_flush(type, group, next_offset / group->file_size, start_lsn);
	}

	/*一个文件存储不下，分片存储*/
	if((next_offset % group->file_size) + len > group->file_size) /*计算本次分片存储的大小*/
		write_len = group->file_size - (next_offset % group->file_size);
	else
		write_len = len;

	if(log_debug_writes){
		printf("Writing log file segment to group %lu offset %lu len %lu\n"
			"start lsn %lu %lu\n",
			group->id, next_offset, write_len,
			ut_dulint_get_high(start_lsn),
			ut_dulint_get_low(start_lsn));

		printf("First block n:o %lu last block n:o %lu\n",
			log_block_get_hdr_no(buf),
			log_block_get_hdr_no(buf + write_len - OS_FILE_LOG_BLOCK_SIZE));

		ut_a(log_block_get_hdr_no(buf) == log_block_convert_lsn_to_no(start_lsn));
		for(i = 0; i < write_len / OS_FILE_LOG_BLOCK_SIZE; i ++){
			ut_a(log_block_get_hdr_no(buf) + i == log_block_get_hdr_no(buf + i * OS_FILE_LOG_BLOCK_SIZE));
		}
	}

	/*计算各个block的check sum， write_len一定是512的整数倍*/
	for(i = 0; i < write_len / OS_FILE_LOG_BLOCK_SIZE; i ++)
		log_block_store_checksum(buf + i * OS_FILE_LOG_BLOCK_SIZE);

	for(log_do_write){
		log_sys->n_log_ios ++;
		/*将block刷入磁盘*/
		fil_io(OS_FILE_WRITE | OS_FILE_LOG, TRUE, group->space_id, next_offset / UNIV_PAGE_SIZE, next_offset % UNIV_PAGE_SIZE, write_len, buf, group);
	}

	if(write_len < len){
		/*更改start lsn*/
		start_lsn = ut_dulint_add(start_lsn, write_len);
		len -= write_len;
		buf += write_len;

		write_header = TRUE;

		goto loop;
	}
}

/*一般是刷log_write_low来的日志*/
void log_flush_up_to(dulint lsn, ulint wait)
{
	log_group_t*	group;
	ulint		start_offset;
	ulint		end_offset;
	ulint		area_start;
	ulint		area_end;
	ulint		loop_count;
	ulint		unlock;

	/*没有ibuf log操作或者正在恢复数据库*/
	if(recv_no_ibuf_operations)
		return ;

	loop_count;

loop:
	loop_count ++;
	ut_ad(loop_count < 5);

	if(loop_count > 2){
		printf("Log loop count %lu\n", loop_count); 
	}

	mutex_enter(&(log_sys->mutex));

	/*lsn <= 已经刷盘的lsn,表示没有数据需要刷盘*/
	if ((ut_dulint_cmp(log_sys->written_to_all_lsn, lsn) >= 0) 
		|| ((ut_dulint_cmp(log_sys->written_to_some_lsn, lsn) >= 0) && (wait != LOG_WAIT_ALL_GROUPS))) {
			mutex_exit(&(log_sys->mutex));

			return;
	}

	/*正在log_sys有fil_flush IO操作*/
	if(log_sys->n_pending_writes > 0){
		if(ut_dulint_cmp(log_sys->flush_lsn, lsn) >= 0)
			goto do_waits;

		mutex_exit(&(log_sys->mutex));

		/*进入no flush event等待*/
		os_event_wait(log_sys->no_flush_event);

		goto loop;
	}

	/*缓冲区中无数据刷盘*/
	if(log_sys->buf_free == log_sys->buf_next_to_write){
		mutex_exit(&(log_sys->mutex));
		return;
	}

	if(log_debug_writes){
		printf("Flushing log from %lu %lu up to lsn %lu %lu\n",
			ut_dulint_get_high(log_sys->written_to_all_lsn),
			ut_dulint_get_low(log_sys->written_to_all_lsn),
			ut_dulint_get_high(log_sys->lsn),
			ut_dulint_get_low(log_sys->lsn));
	}

	log_sys->n_pending_writes++;

	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	group->n_pending_writes++; 

	/*重置信号，以便对这两个信号进行监视*/
	os_event_reset(log_sys->no_flush_event);
	os_event_reset(log_sys->one_flushed_event);

	/*计算需要flush的位置范围*/
	start_offset = log_sys->buf_next_to_write;
	end_offset = log_sys->buf_free;

	area_start = ut_calc_align_down(start_offset, OS_FILE_LOG_BLOCK_SIZE);
	area_end = ut_calc_align(end_offset, OS_FILE_LOG_BLOCK_SIZE);

	ut_ad(area_end - area_start > 0);

	/*设置最后flush的lsn*/
	log_sys->flush_lsn = log_sys->lsn;
	log_sys->one_flushed = FALSE;

	/*设置flush的标识位*/
	log_block_set_flush_bit(log_sys->buf + area_start, TRUE);
	/*在最后一块设置了checkpoint no*/
	log_block_set_checkpoint_no(log_sys->buf + area_end - OS_FILE_LOG_BLOCK_SIZE, log_sys->next_checkpoint_no);

	/*为什么将最后一个block向后移动,有可能最后一个block并没有满，防止下次写不会写在最后flush的这个块上，因为buf_free 和flush_end_offset做了增长？？*/
	ut_memcpy(log_sys->buf + area_end, log_sys->buf + area_end - OS_FILE_LOG_BLOCK_SIZE, OS_FILE_LOG_BLOCK_SIZE);
	log_sys->buf_free += OS_FILE_LOG_BLOCK_SIZE;
	log_sys->flush_end_offset = log_sys->buf_free;

	/*将各个group buf刷入到磁盘*/
	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	while(group){
		/*将group buf刷入到log文件中*/
		log_group_write_buf(LOG_FLUSH, group, log_sys->buf + area_start, area_end - area_start, 
			ut_dulint_align_down(log_sys->written_to_all_lsn, OS_FILE_LOG_BLOCK_SIZE), start_offset - area_start);

		/*设置新的group->lsn*/
		log_group_set_fields(group, log_sys->flush_lsn);
		group = UT_LIST_GET_NEXT(log_groups, group);
	}

	mutex_exit(&(log_sys->mutex));

	/*srv_flush_log_at_trx_commit =2的话，日志只是在PAGE CACHE当中，如果服务器断电重启，日志就丢了*/
	if (srv_unix_file_flush_method != SRV_UNIX_O_DSYNC && srv_unix_file_flush_method != SRV_UNIX_NOSYNC && srv_flush_log_at_trx_commit != 2) {
			group = UT_LIST_GET_FIRST(log_sys->log_groups);
			fil_flush(group->space_id);
	}

	mutex_enter(&(log_sys->mutex));
	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	ut_a(group->n_pending_writes == 1);
	ut_a(log_sys->n_pending_writes == 1);

	group->n_pending_writes--;
	log_sys->n_pending_writes--;

	/*检测io是否完成*/
	unlock = log_group_check_flush_completion(group);
	unlock = unlock | log_sys_check_flush_completion();
	log_flush_do_unlocks(unlock);

	mutex_exit(log_sys->mutex);
	return;

do_waits:
	mutex_exit(&(log_sys->mutex));

	/*等待操作完成的信号*/
	if(wait == LOG_WAIT_ONE_GROUP)
		os_event_wait(log_sys->one_flushed_event);
	else if(wait == LOG_WAIT_ALL_GROUPS)
		os_event_wait(log_sys->no_flush_event);
	else
		ut_ad(wait == LOG_NO_WAIT);
}

static void log_flush_margin()
{
	ibool	do_flush = FALSE;
	log_t*	log = log_sys;

	mutex_enter(&(log->mutex));
	if(log->buf_free > log->max_buf_free){ /*已经超过了容忍的最大位置，必须进行强制刷盘*/
		if(log->n_pending_writes > 0){ /*已经有fil_flush在刷盘*/

		}
		else
			do_flush = TRUE;
	}

	mutex_exit(&(log->mutex));
	if(do_flush) /*强制将log_sys刷盘*/
		log_flush_up_to(ut_dulint_max, LOG_NO_WAIT);
}

/********************************************************************
Advances the smallest lsn for which there are unflushed dirty blocks in the
buffer pool. NOTE: this function may only be called if the calling thread owns
no synchronization objects! */
ibool log_preflush_pool_modified_pages(dulint new_oldest, ibool sync)
{
	ulint n_pages;

	if(recv_recovery_on){ /*正在进行数据恢复，必须将所有的记录的log加入到他们搁置的file page里面要获得正确的lsn*/
		recv_apply_hashed_log_recs(TRUE);
	}

	n_pages = buf_flush_batch(BUF_FLUSH_LIST, ULINT_MAX, new_oldest);
	if(sync)
		buf_flush_wait_batch_end(BUF_FLUSH_LIST);
	
	return (n_pages == ULINT_UNDEFINED) ? FALSE : TRUE;
}

static void log_complete_checkpoint()
{
	ut_ad(mutex_own(&(log_sys->mutex)));
	ut_ad(log_sys->n_pending_checkpoint_writes == 0);

	/*checkpoint ++*/
	log_sys->next_checkpoint_no = ut_dulint_add(log_sys->next_checkpoint_no, 1);
	/*更改last checkpoint lsn*/
	log_sys->last_checkpoint_lsn = log_sys->next_checkpoint_lsn;

	/*释放独占的checkpoint_lock*/
	rw_lock_x_unlock_gen(&(log_sys->checkpoint_lock), LOG_CHECKPOINT);
}

static void log_io_complete_checkpoint(log_group_t* group)
{
	mutex_enter(&(log_sys->mutex));

	ut_ad(log_sys->n_pending_checkpoint_writes > 0);
	log_sys->n_pending_checkpoint_writes --;

	if(log_debug_writes)
		printf("Checkpoint info written to group %lu\n", group->id);
	
	/*已经完成了所有check io操作*/
	if(log_sys->n_pending_checkpoint_writes == 0)
		log_complete_checkpoint();

	mutex_exit(&(log_sys->mutex));
}

/*设置checkpoint位置*/
static void log_checkpoint_set_nth_group_info(byte* buf, ulint n, ulint file_no, ulint offset)
{
	ut_ad(n < LOG_MAX_N_GROUPS);
	/*将checkpoint的文件和对应位置写入buf当中*/
	mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_ARCHIVED_FILE_NO, file_no);
	mach_write_to_4(buf + LOG_CHECKPOINT_GROUP_ARRAY + 8 * n + LOG_CHECKPOINT_ARCHIVED_OFFSET, offset);
}

/*从buf中获取一个checkpoint的文件位置*/
void log_checkpoint_get_nth_group_info(byte* buf, ulint	n, ulint* file_no, ulint* offset)	
{
	ut_ad(n < LOG_MAX_N_GROUPS);

	*file_no = mach_read_from_4(buf + LOG_CHECKPOINT_GROUP_ARRAY
		+ 8 * n + LOG_CHECKPOINT_ARCHIVED_FILE_NO);

	*offset = mach_read_from_4(buf + LOG_CHECKPOINT_GROUP_ARRAY
		+ 8 * n + LOG_CHECKPOINT_ARCHIVED_OFFSET);
}

/*将checkpoint信息写入到group header缓冲区*/
static void log_group_checkpoint(log_group_t* group)
{
	log_group_t*	group2;
	dulint	archived_lsn;
	dulint	next_archived_lsn;
	ulint	write_offset;
	ulint	fold;
	byte*	buf;
	ulint	i;

	ut_ad(mutex_own(&(log_sys->mutex)));
	ut_a(LOG_CHECKPOINT_SIZE <= OS_FILE_LOG_BLOCK_SIZE);

	buf = group->checkpoint_buf;
	/*写入checkpoint no*/
	mach_write_to_8(buf + LOG_CHECKPOINT_NO, log_sys->next_checkpoint_no);
	/*写入checkpoint lsn*/
	mach_write_to_8(buf + LOG_CHECKPOINT_LSN, log_sys->next_checkpoint_lsn);
	/*next_checkpoint_lsn相对group 的起始位置*/
	mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET,log_group_calc_lsn_offset(log_sys->next_checkpoint_lsn, group));

	/*归档状态为关闭，设置为最大的lsn*/
	if(log_sys->archiving_state == LOG_ARCH_OFF)
		archived_lsn = ut_dulint_max;
	else{
		archived_lsn = log_sys->archived_lsn;
		if (0 != ut_dulint_cmp(archived_lsn, log_sys->next_archived_lsn))
				next_archived_lsn = log_sys->next_archived_lsn;
	}
	mach_write_to_8(buf + LOG_CHECKPOINT_ARCHIVED_LSN, archived_lsn);

	/*初始化每个group checkpoint位置信息*/
	for(i = 0; i < LOG_MAX_N_GROUPS; i ++){
		log_checkpoint_set_nth_group_info(buf, i, 0, 0);
	}

	group2 = UT_LIST_GET_FIRST(log_sys->log_groups);
	while (group2) {
		/*设置每个group的checkpoint位置信息*/
		log_checkpoint_set_nth_group_info(buf, group2->id, group2->archived_file_no, group2->archived_offset);
		group2 = UT_LIST_GET_NEXT(log_groups, group2);
	}

	fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
	mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_1, fold); /*用hash作为checksum*/
	fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
	mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_2, fold); /*用除checkpoint no以外的信息计算的checksum(会包括checksum1)*/
	/*设置free limit*/
	mach_write_to_4(buf + LOG_CHECKPOINT_FSP_FREE_LIMIT, log_fsp_current_free_limit);
	/*设置魔法字*/
	mach_write_to_4(buf + LOG_CHECKPOINT_FSP_MAGIC_N, LOG_CHECKPOINT_FSP_MAGIC_N_VAL);

	if (ut_dulint_get_low(log_sys->next_checkpoint_no) % 2 == 0)
		write_offset = LOG_CHECKPOINT_1;
	else 
		write_offset = LOG_CHECKPOINT_2;

	if(log_do_write){
		if(log_sys->n_pending_checkpoint_writes == 0) /*没有检查点在IO操作，n_pending_checkpoint_writes在checkpoint完成的时候会--*/
			rw_lock_x_lock_gen(&(log_sys->checkpoint_lock), LOG_CHECKPOINT);

		log_sys->n_pending_checkpoint_writes ++;
		log_sys->n_log_ios ++;

		/*写入group space的0 ~ 2048中，如果是LOG_CHECKPOINT_1从0（文件的第一个扇区）偏移开始写入，如果是LOG_CHECKPOINT_2从1536（文件的第4个扇区）偏移处开始写*/
		fil_io(OS_FILE_WRITE | OS_FILE_LOG, FALSE, group->space_id, write_offset / UNIV_PAGE_SIZE, write_offset % UNIV_PAGE_SIZE, OS_FILE_LOG_BLOCK_SIZE,
			buf, ((byte*)group + 1));

		ut_ad(((ulint)group & 0x1) == 0);
	}
}

/*当log files建立的时候，需要在更新group file头信息和checkpoint信息*/
void log_reset_first_header_and_checkpoint(byte* hdr_buf, dulint start)
{
	ulint	fold;
	byte*	buf;
	dulint	lsn;

	mach_write_to_4(hdr_buf + LOG_GROUP_ID, 0);	
	mach_write_to_8(hdr_buf + LOG_FILE_START_LSN, start); /*重做日志中第一个日志的lsn*/

	lsn = ut_dulint_add(start, LOG_BLOCK_HDR_SIZE);

	/*写入一个与时间戳关联的ibbackup标志*/
	sprintf(hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "ibbackup ");
	ut_sprintf_timestamp(hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP + strlen("ibbackup "));

	buf = hdr_buf + LOG_CHECKPOINT_1;
	mach_write_to_8(buf + LOG_CHECKPOINT_NO, ut_dulint_zero);
	mach_write_to_8(buf + LOG_CHECKPOINT_LSN, lsn);

	mach_write_to_4(buf + LOG_CHECKPOINT_OFFSET, LOG_FILE_HDR_SIZE + LOG_BLOCK_HDR_SIZE);

	mach_write_to_4(buf + LOG_CHECKPOINT_LOG_BUF_SIZE, 2 * 1024 * 1024);

	mach_write_to_8(buf + LOG_CHECKPOINT_ARCHIVED_LSN, ut_dulint_max);

	/*计算一个checksum*/
	fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
	mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_1, fold);

	fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
	mach_write_to_4(buf + LOG_CHECKPOINT_CHECKSUM_2, fold);
}

/*从group header头信息中读取checkpoint到log_sys->checkpoint_buf当中*/
void log_group_read_checkpoint_info(log_group_t* group)
{
	ut_ad(mutex_own(&(log_sys->mutex)));
	log_sys->n_log_ios ++;

	/*文件读取*/
	file_io(OS_FILE_READ | OS_FILE_LOG, TRUE, group->space_id, field / UNIV_PAGE_SIZE, field % UNIV_PAGE_SIZE, OS_FILE_LOG_BLOCK_SIZE,
		log_sys->checkpoint_buf, NULL);
}

void log_groups_write_checkpoint_info()
{
	log_group_t* group;
	ut_ad(mutex_own(&(log_sys->mutex)));

	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	while(group != NULL){
		/*将group的checkpoint信息写入到磁盘上*/
		log_group_checkpoint(group);
		group = UT_LIST_GET_NEXT(log_groups, group);
	}
}

ibool log_checkpoint(ibool sync, ibool write_always)
{
	dulint oldest_lsn;
	if(recv_recovery_is_on()){ /*正在日志恢复*/
		recv_apply_hashed_log_recs(TRUE);
	}

	/*表空间文件刷盘*/
	if(srv_unix_file_flush_method != SRV_UNIX_NOSYNC)
		fil_flush_file_spaces(FIL_TABLESPACE);

	mutex_enter(&(log_sys->mutex));
	/*获得ibuff中的oldest lsn*/
	oldest_lsn = log_buf_pool_get_oldest_modification();
	mutex_exit(&(log_sys->mutex));

	/*将group中的的page和日志全部进行刷盘*/
	log_flush_up_to(oldest_lsn, LOG_WAIT_ALL_GROUPS);

	mutex_enter(&(log_sys->mutex));

	/*最近的checkpoint大于oldest_lsn,不需要进行checkpoint*/
	if (!write_always && ut_dulint_cmp(log_sys->last_checkpoint_lsn, oldest_lsn) >= 0){
			mutex_exit(&(log_sys->mutex));

			return(TRUE);
	}

	ut_ad(ut_dulint_cmp(log_sys->written_to_all_lsn, oldest_lsn) >= 0);
	if(log_sys->n_pending_checkpoint_writes > 0){ /*已经有一个checkpoint在进行io写*/
		mutex_exit(&(log_sys->mutex));

		if(sync){ /*等待这个checkpoint完成，因为在checkpoint的时候，会独占checkpoint_lock*/
			rw_lock_s_lock(&(log_sys->checkpoint_lock));
			rw_lock_s_unlock(&(log_sys->checkpoint_lock));
		}

		return FALSE;
	}

	log_sys->next_checkpoint_lsn = oldest_lsn;
	if (log_debug_writes) {
		printf("Making checkpoint no %lu at lsn %lu %lu\n", ut_dulint_get_low(log_sys->next_checkpoint_no), ut_dulint_get_high(oldest_lsn),
			ut_dulint_get_low(oldest_lsn));
	}

	/*写入checkpoint的信息到磁盘*/
	log_groups_write_checkpoint_info();

	mutex_exit(&(log_sys->mutex));

	if (sync) { /*等待io完成, 在log_groups_write_checkpoint_info会占有checkpoint_lock*/
		rw_lock_s_lock(&(log_sys->checkpoint_lock));
		rw_lock_s_unlock(&(log_sys->checkpoint_lock));
	}

	return TRUE;
}

void log_make_checkpoint_at(dulint lsn, ibool write_always)
{
	ibool success = FALSE;

	/*为page刷盘做预处理*/
	while(!success)
		success = log_preflush_pool_modified_pages(lsn, TRUE);

	success = FALSE;
	while(!success) /*尝试建立新的checkpoint*/
		success = log_checkpoint(TRUE, write_always);
}

/*判断是否需要对page刷盘*/
static void log_checkpoint_margin()
{
	log_t*	log		= log_sys;
	ulint	age;
	ulint	checkpoint_age;
	ulint	advance;
	dulint	oldest_lsn;
	dulint	new_oldest;
	ibool	do_preflush;
	ibool	sync;
	ibool	checkpoint_sync;
	ibool	do_checkpoint;
	ibool	success;

loop:
	sync = FALSE;
	checkpoint_sync = FALSE;
	do_preflush = FALSE;
	do_checkpoint = FALSE;

	mutex_enter(&(log->mutex));

	/*不需要建立checkpoint*/
	if(!log->check_flush_or_checkpoint){
		mutex_exit(&(log->mutex));
		return;
	}

	oldest_lsn = log_buf_pool_get_oldest_modification();
	age = ut_dulint_minus(log->lsn, oldest_lsn);

	if(age > log->max_modified_age_sync){ /*超过了page同步flush的阈值*/
		sync = TRUE;
		advance = 2 * (age - log->max_modified_age_sync);
		new_oldest = ut_dulint_add(oldest_lsn, advance);

		do_preflush = TRUE;
	}
	else if(age > log->max_modified_age_async){ /*超过了page异步flush的阈值*/
		advance = age - log->max_modified_age_async;
		new_oldest = ut_dulint_add(oldest_lsn, advance);
		do_preflush = TRUE;
	}
	
	/*检查是否触发了checkpoint建立的阈值*/
	checkpoint_age = ut_dulint_minus(log->lsn, log->last_checkpoint_lsn);
	if (checkpoint_age > log->max_checkpoint_age) {
		checkpoint_sync = TRUE;
		do_checkpoint = TRUE;

	} 
	else if (checkpoint_age > log->max_checkpoint_age_async) { /*建立checkpoint并异步刷盘*/
		do_checkpoint = TRUE;
		log->check_flush_or_checkpoint = FALSE;
	} 
	else {
		log->check_flush_or_checkpoint = FALSE;
	}

	if(do_preflush){
		success = log_preflush_pool_modified_pages(new_oldest, sync);
		if(sync && !success){ /*进行重试*/
			mutex_enter(&(log->mutex));
			log->check_flush_or_checkpoint = TRUE;
			mutex_exit(&(log->mutex));
			goto loop;
		}
	}

	if (do_checkpoint) {
		/*创建checkpoint*/
		log_checkpoint(checkpoint_sync, FALSE);
		if (checkpoint_sync) /*重新检查是否要其他的刷盘操作*/
			goto loop;
	}
}

/*读取一段特定的log到缓冲区中*/
void log_group_read_log_seg(ulint type, byte* buf, log_group_t* group, dulint start_lsn, dulint end_lsn)
{
	ulint	len;
	ulint	source_offset;
	ibool	sync;

	ut_ad(mutex_own(&(log_sys->mutex)));

	sync = FALSE;
	if(type == LOG_RECOVER) /*是恢复过程的读取*/
		sync = TRUE;

loop:
	source_offset = log_group_calc_lsn_offset(start_lsn, group);
	len = ut_dulint_minus(end_lsn, start_lsn);

	ut_ad(len != 0);
	/*如果len长度过大，让其正好能防止在goup file的剩余空间里面*/
	if((source_offset % group->file_size) + len > group->file_size)
		len = group->file_size - (source_offset % group->file_size);

	/*进行io操作统计*/
	if(type == LOG_ARCHIVE)
		log_sys->n_pending_archive_ios ++;
	
	log_sys->n_log_ios ++;

	fil_io(OS_FILE_READ | OS_FILE_LOG, sync, group->space_id,source_offset / UNIV_PAGE_SIZE, source_offset % UNIV_PAGE_SIZE,
		len, buf, &log_archive_io);

	start_lsn = ut_dulint_add(start_lsn, len);
	buf += len;

	if (ut_dulint_cmp(start_lsn, end_lsn) != 0) /*没有达到预期读取的长度，继续*/
		goto loop;
}

void log_archived_file_name_gen(char* buf, ulint id, ulint file_no)
{
	UT_NOT_USED(id);
	sprintf(buf, "%sib_arch_log_%010lu", srv_arch_dir, file_no);
}

/*将archive header写入log file*/
static void log_group_archive_file_header_write(log_group_t* group, ulint nth_file, ulint file_no, dulint start_lsn)
{
	byte* buf;
	ulint dest_offset;

	ut_ad(mutex_own(&(log_sys->mutex)));
	ut_a(nth_file < group->n_files);

	buf = *(group->archive_file_header_bufs + nth_file);

	mach_write_to_4(buf + LOG_GROUP_ID, group->id);
	mach_write_to_8(buf + LOG_FILE_START_LSN, start_lsn);
	mach_write_to_4(buf + LOG_FILE_NO, file_no);

	mach_write_to_4(buf + LOG_FILE_ARCH_COMPLETED, FALSE);

	dest_offset = nth_file * group->file_size;

	log_sys->n_log_ios ++;

	fil_io(OS_FILE_WRITE | OS_FILE_LOG, TRUE, group->archive_space_id, dest_offset / UNIV_PAGE_SIZE, dest_offset % UNIV_PAGE_SIZE, 
		2 * OS_FILE_LOG_BLOCK_SIZE, buf, &log_archive_io);
}

/*修改log file header表示已经完成log file归档*/
static void log_group_archive_completed_header_write(log_group_t* group, ulint nth_file, dulint end_lsn)
{
	byte*	buf;
	ulint	dest_offset;

	ut_ad(mutex_own(&(log_sys->mutex)));
	ut_a(nth_file < group->n_files);

	buf = *(group->archive_file_header_bufs + nth_file);
	mach_write_to_4(buf + LOG_FILE_ARCH_COMPLETED, TRUE);
	mach_write_to_8(buf + LOG_FILE_END_LSN, end_lsn);

	dest_offset = nth_file * group->file_size + LOG_FILE_ARCH_COMPLETED;

	fil_io(OS_FILE_WRITE | OS_FILE_LOG, TRUE, group->archive_space_id, dest_offset / UNIV_PAGE_SIZE, dest_offset % UNIV_PAGE_SIZE,
		OS_FILE_LOG_BLOCK_SIZE, buf + LOG_FILE_ARCH_COMPLETED, &log_archive_io);
}

static void log_group_archive(log_group_t* group)
{
	os_file_t file_handle;
	dulint	start_lsn;
	dulint	end_lsn;
	char	name[100];
	byte*	buf;
	ulint	len;
	ibool	ret;
	ulint	next_offset;
	ulint	n_files;
	ulint	open_mode;

	ut_ad(mutex_own(&(log_sys->mutex)));

	/*计算归档的起始位置和终止位置*/
	start_lsn = log_sys->archived_lsn;
	end_lsn = log_sys->next_archived_lsn;
	ut_ad(ut_dulint_get_low(start_lsn) % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_ad(ut_dulint_get_low(end_lsn) % OS_FILE_LOG_BLOCK_SIZE == 0);

	buf = log_sys->archive_buf;

	n_files = 0;
	next_offset = group->archived_offset;

loop:
	/*判断是否需要新建一个新的archive file*/
	if((next_offset % group->file_size == 0) || (fil_space_get_size(group->archive_space_id) == 0)){
		if(next_offset % group->file_size == 0)
			open_mode = OS_FILE_CREATE; /*新建并打开一个文件*/
		else
			open_mode = OS_FILE_OPEN;	/*仅仅打开一个文件*/

		log_archived_file_name_gen(name, group->id, group->archived_file_no + n_files);
		fil_reserve_right_to_open();

		file_handle = os_file_create(name, open_mode, OS_FILE_AIO, OS_DATA_FILE, &ret);
		if (!ret && (open_mode == OS_FILE_CREATE)) /*要创建的文件已经存在，更改打开模式*/
			file_handle = os_file_create(name, OS_FILE_OPEN, OS_FILE_AIO, OS_DATA_FILE, &ret);
		/*磁盘无法建立或者打开文件*/
		if (!ret) {
			fprintf(stderr, "InnoDB: Cannot create or open archive log file %s.\n",name);
			fprintf(stderr, "InnoDB: Cannot continue operation.\n"
				"InnoDB: Check that the log archive directory exists,\n"
				"InnoDB: you have access rights to it, and\n"
				"InnoDB: there is space available.\n");
			exit(1);
		}

		if (log_debug_writes)
			printf("Created archive file %s\n", name);

		ret = os_file_close(file_handle);
		ut_a(ret);

		fil_release_right_to_open();
		fil_node_create(name, group->file_size / UNIV_PAGE_SIZE, group->archive_space_id);

		if(next_offset % group->file_size == 0){ /*新建的归档文件*/
			log_group_archive_file_header_write(group, n_files, group->archived_file_no + n_files, start_lsn);
			next_offset += LOG_FILE_HDR_SIZE;
		}
	}

	len = ut_dulint_minus(end_lsn, start_lsn);
	/*进行分片存储*/
	if (group->file_size < (next_offset % group->file_size) + len) /*文件空闲空间无法存放len长度的数据*/
		len = group->file_size - (next_offset % group->file_size);

	if (log_debug_writes) {
		printf("Archiving starting at lsn %lu %lu, len %lu to group %lu\n",
			ut_dulint_get_high(start_lsn), ut_dulint_get_low(start_lsn), len, group->id);
	}

	log_sys->n_pending_archive_ios ++;
	log_sys->n_log_ios ++;

	fil_io(OS_FILE_WRITE | OS_FILE_LOG, FALSE, group->archive_space_id,
		next_offset / UNIV_PAGE_SIZE, next_offset % UNIV_PAGE_SIZE,
		ut_calc_align(len, OS_FILE_LOG_BLOCK_SIZE), buf, &log_archive_io);

	start_lsn = ut_dulint_add(start_lsn, len);
	next_offset += len;
	buf += len;

	if (next_offset % group->file_size == 0)
		n_files++;

	if(ut_dulint_cmp(end_lsn, start_lsn) != 0)
		goto loop;

	group->next_archived_file_no = group->archived_file_no + n_files;
	group->next_archived_offset = next_offset % group->file_size;
	/*next_archived_offset一定是OS_FILE_LOG_BLOCK_SIZE对齐的*/
	ut_ad(group->next_archived_offset % OS_FILE_LOG_BLOCK_SIZE == 0);
}

static void log_archive_groups()
{
	log_group_t* group;
	ut_ad(mutex_own(&(log_sys->mutex)));

	/*对group进行归档*/
	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	log_group_archive(group);
}

static void log_archive_write_complete_groups()
{
	log_group_t*	group;
	ulint		end_offset;
	ulint		trunc_files;
	ulint		n_files;
	dulint		start_lsn;
	dulint		end_lsn;
	ulint		i;

	ut_ad(mutex_own(&(log_sys->mutex)));

	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	group->archived_file_no = group->next_archived_file_no;
	group->archived_offset = group->next_archived_offset;

	/*获得已经归档的文件*/
	n_files = (UNIV_PAGE_SIZE * fil_space_get_size(group->archive_space_id)) / group->file_size;
	ut_ad(n_files > 0);

	end_offset = group->archived_offset;
	if(end_offset % group->file_size == 0) /*前面的文件没有空闲*/
		trunc_files = n_files;
	else
		trunc_files = n_files - 1;

	if (log_debug_writes && trunc_files)
		printf("Complete file(s) archived to group %lu\n", group->id);

	start_lsn = ut_dulint_subtract(log_sys->next_archived_lsn, end_offset - LOG_FILE_HDR_SIZE + trunc_files * (group->file_size - LOG_FILE_HDR_SIZE));
	end_lsn = start_lsn;

	for(i = 0;i < trunc_files; i ++){
		end_lsn = ut_dulint_add(end_lsn, group->file_size - LOG_FILE_HDR_SIZE);
		/*修改该归档完成的信息*/
		log_group_archive_completed_header_write(group, i, end_lsn);
	}

	fil_space_truncate_start(group->archive_space_id, trunc_files * group->file_size);

	if(log_debug_writes)
		printf("Archiving writes completed\n");
}

static void log_archive_check_completion_low()
{
	ut_ad(mutex_own(&(log_sys->mutex)));
	if(log_sys->n_pending_archive_ios == 0 && log_sys->archiving_phase == LOG_ARCHIVE_READ){
		if (log_debug_writes) /*归档的数据读过程已经完成*/
			printf("Archiving read completed\n");

		/*进入写阶段*/
		log_sys->archiving_phase = LOG_ARCHIVE_WRITE;
		log_archive_groups();
	}

	/*完成archive io的过程*/
	if(log_sys->n_pending_archive_ios == 0 && log_sys->archiving_phase == LOG_ARCHIVE_WRITE){
		log_archive_write_complete_groups();
		/*已经完成归档，释放archive_lock*/
		log_sys->archived_lsn = log_sys->next_archived_lsn;
		rw_lock_x_unlock_gen(&(log_sys->archive_lock), LOG_ARCHIVE);
	}
}

static void log_io_complete_archive()
{
	log_group_t*	group;
	mutex_enter(&(log_sys->mutex));
	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	mutex_exit(&(log_sys->mutex));

	/*对归档文件进行刷盘*/
	fil_flush(group->archive_space_id);

	mutex_enter(&(log_sys->mutex));
	ut_ad(log_sys->n_pending_archive_ios > 0);
	log_sys->n_pending_archive_ios --;

	/*修改归档文件头状态*/
	log_archive_check_completion_low();
	mutex_exit(&(log_sys->mutex));
}

/**/
ibool log_archive_do(ibool sync, ulint* n_bytes)
{
	ibool	calc_new_limit;
	dulint	start_lsn;
	dulint	limit_lsn;

	calc_new_limit = TRUE;

loop:
	mutex_enter(&(log_sys->mutex));

	if (log_sys->archiving_state == LOG_ARCH_OFF) { /*归档被关闭*/
		mutex_exit(&(log_sys->mutex));
		*n_bytes = 0;
		return(TRUE);
	}
	else if(log_sys->archiving_state == LOG_ARCH_STOPPED || log_sys->archiving_state == LOG_ARCH_STOPPING2){
		mutex_exit(&(log_sys->mutex));
		os_event_wait(log_sys->archiving_on); /*等待完成信号*/
		
		/*mutex_enter(&(log_sys->mutex));*/
		goto loop;
	}

	/*确定start_lsn 和 end_lsn*/
	start_lsn = log_sys->archived_lsn;
	if(calc_new_limit){
		ut_ad(log_sys->archive_buf_size % OS_FILE_LOG_BLOCK_SIZE == 0);
		limit_lsn = ut_dulint_add(start_lsn, log_sys->archive_buf_size);
		/*不能超过当前log的lsn*/
		if(ut_dulint_cmp(limit_lsn, log_sys->lsn) >= 0)
			limit_lsn = ut_dulint_align_down(log_sys->lsn, OS_FILE_LOG_BLOCK_SIZE);
	}

	/*无任何数据归档*/
	if(ut_dulint_cmp(log_sys->archived_lsn, limit_lsn) >= 0){
		mutex_exit(&(log_sys->mutex));
		*n_bytes = 0;
		return(TRUE);
	}

	/*所有的group lsn小于limit_lsn，先进行log写盘，使得written_to_all_lsn不小于limit_lsn，这样才能进行归档操作*/
	if (ut_dulint_cmp(log_sys->written_to_all_lsn, limit_lsn) < 0) {
		mutex_exit(&(log_sys->mutex));
		log_flush_up_to(limit_lsn, LOG_WAIT_ALL_GROUPS);
		calc_new_limit = FALSE;

		goto loop;
	}

	if(log_sys->n_pending_archive_ios > 0){
		mutex_exit(&(log_sys->mutex));
		if(sync){ /*等待正在进行的归档完成*/
			rw_lock_s_lock(&(log_sys->archive_lock));
			rw_lock_s_unlock(&(log_sys->archive_lock));
		}

		*n_bytes = log_sys->archive_buf_size;
		return FALSE;
	}
	/*对archive_lock加独占锁*/
	rw_lock_x_lock_gen(&(log_sys->archive_lock), LOG_ARCHIVE);
	log_sys->archiving_phase = LOG_ARCHIVE_READ;
	log_sys->next_archived_lsn = limit_lsn;

	if(log_debug_writes)
		printf("Archiving from lsn %lu %lu to lsn %lu %lu\n",
		ut_dulint_get_high(log_sys->archived_lsn),
		ut_dulint_get_low(log_sys->archived_lsn),
		ut_dulint_get_high(limit_lsn),
		ut_dulint_get_low(limit_lsn));

	/*读取需要归档的数据*/
	log_group_read_log_seg(LOG_ARCHIVE, log_sys->archive_buf, UT_LIST_GET_FIRST(log_sys->log_groups), start_lsn, limit_lsn);
	mutex_exit(&(log_sys->mutex));

	/*等待完成*/
	if (sync) {
		rw_lock_s_lock(&(log_sys->archive_lock));
		rw_lock_s_unlock(&(log_sys->archive_lock));
	}
	
	*n_bytes = log_sys->archive_buf_size;

	return TRUE;
}

static void log_archive_all(void)
{
	dulint	present_lsn;
	ulint	dummy;

	mutex_enter(&(log_sys->mutex));

	if (log_sys->archiving_state == LOG_ARCH_OFF) { /*归档操作关闭*/
		mutex_exit(&(log_sys->mutex));
		return;
	}

	present_lsn = log_sys->lsn;

	mutex_exit(&(log_sys->mutex));

	log_pad_current_log_block();

	for (;;) {
		mutex_enter(&(log_sys->mutex));

		/*没有到归档范围不足，直接退出*/
		if (ut_dulint_cmp(present_lsn, log_sys->archived_lsn) <= 0) {
			mutex_exit(&(log_sys->mutex));

			return;
		}

		mutex_exit(&(log_sys->mutex));
		/*发起一个归档操作*/
		log_archive_do(TRUE, &dummy);
	}
}	

static void log_archive_close_groups(ibool increment_file_count)
{
	log_group_t* group;
	ulint	trunc_len;

	ut_ad(mutex_own(&(log_sys->mutex)));
	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	trunc_len = UNIV_PAGE_SIZE * fil_space_get_size(group->archive_space_id);
	if(trunc_len > 0){
		ut_a(trunc_len == group->file_size);
		/*标志归档文件头信息为完成归档状态*/
		log_group_archive_completed_header_write(group, 0, log_sys->archived_lsn);

		fil_space_truncate_start(group->archive_space_id, trunc_len);

		if(increment_file_count){
			group->archived_offset = 0;
			group->archived_file_no += 2;
		}
		
		if(log_debug_writes)
			printf("Incrementing arch file no to %lu in log group %lu\n", group->archived_file_no + 2, group->id);
	}
}

/*完成所有的archive操作*/
ulint log_archive_stop()
{
	ibool success;
	mutex_enter(&(log_sysy->mutex));

	if(log_sys->archiving_state != LOG_ARCH_ON){
		mutex_exit(&(log_sys->mutex));
		return(DB_ERROR);
	}

	log_sys->archiving_state = LOG_ARCH_STOPPING;
	mutex_exit(&(log_sys->mutex));

	log_archive_all();

	mutex_enter(&(log_sys->mutex));
	log_sys->archiving_state = LOG_ARCH_STOPPING2;
	os_event_reset(log_sys->archiving_on);
	mutex_exit(&(log_sys->mutex));

	/*同步等待archive_lock完成*/
	rw_lock_s_lock(&(log_sys->archive_lock));
	rw_lock_s_unlock(&(log_sys->archive_lock));

	log_archive_close_groups(TRUE);

	mutex_exit(&(log_sys->mutex));
	
	/*强制进行checkpoint*/
	success = FALSE;
	while(!success)
		success = log_checkpoint(TRUE, TRUE);

	mutex_enter(&(log_sys->mutex));
	log_sys->archiving_state = LOG_ARCH_STOPPED;
	mutex_exit(&(log_sys->mutex));

	return DB_SUCCESS;
}
/*由LOG_ARCH_STOPPED开启LOG_ARCH_ON*/
ulint log_archive_start()
{
	mutex_enter(&(log_sys->mutex));

	if (log_sys->archiving_state != LOG_ARCH_STOPPED) {
		mutex_exit(&(log_sys->mutex));
		return(DB_ERROR);
	}	

	/*重启archive*/
	log_sys->archiving_state = LOG_ARCH_ON;
	os_event_set(log_sys->archiving_on);
	mutex_exit(&(log_sys->mutex));

	return(DB_SUCCESS);
}

/*关闭archive机制*/
ulint log_archive_noarchivelog(void)
{
loop:
	mutex_enter(&(log_sys->mutex));

	if (log_sys->archiving_state == LOG_ARCH_STOPPED
		|| log_sys->archiving_state == LOG_ARCH_OFF) {

			log_sys->archiving_state = LOG_ARCH_OFF;
			os_event_set(log_sys->archiving_on);
			mutex_exit(&(log_sys->mutex));

			return(DB_SUCCESS);
	}	

	mutex_exit(&(log_sys->mutex));
	/*完成所有的archive操作*/
	log_archive_stop();
	os_thread_sleep(500000);

	goto loop;	
}
/*由LOG_ARCH_OFF开启LOG_ARCH_ON*/
ulint log_archive_archivelog(void)
{
	mutex_enter(&(log_sys->mutex));

	/*重新开启LOG_ARCH_ON*/
	if (log_sys->archiving_state == LOG_ARCH_OFF) {
		log_sys->archiving_state = LOG_ARCH_ON;
		log_sys->archived_lsn = ut_dulint_align_down(log_sys->lsn, OS_FILE_LOG_BLOCK_SIZE);	
		mutex_exit(&(log_sys->mutex));

		return(DB_SUCCESS);
	}	

	mutex_exit(&(log_sys->mutex));
	return(DB_ERROR);	
}

/*archive操作触发检测*/
static void log_archive_margin()
{
	log_t*	log		= log_sys;
	ulint	age;
	ibool	sync;
	ulint	dummy;

loop:
	mutex_enter(&(log_sys->mutex));
	/*archive关闭状态*/
	if (log->archiving_state == LOG_ARCH_OFF) {
		mutex_exit(&(log->mutex));

		return;
	}

	/*检查触发条件*/
	age = ut_dulint_minus(log->lsn, log->archived_lsn);
	if(age > log->max_archived_lsn_age) /*同步方式开始archive操作*/
		sync = TRUE;
	else if(age > log->max_archived_lsn_age_async){/*异步方式开始archive操作*/
		sync = FALSE;
	}
	else{
		mutex_exit(&(log->mutex));
		return ;
	}

	mutex_exit(&(log->mutex));
	log_archive_do(sync, &dummy);
	if(sync)
		goto loop;
}

/*检查是否可以刷盘或者建立checkpoint*/
void log_check_margins()
{
loop:
	/*检查日志文件是否刷盘*/
	log_flush_margin();
	/*检查是否触发建立checkpoint*/
	log_checkpoint_margin();
	/*检查是否可以触发归档操作*/
	log_archive_margin();

	mutex_enter(&(log_sys->mutex));
	if (log_sys->check_flush_or_checkpoint){
		mutex_exit(&(log_sys->mutex));
		goto loop;
	}
	mutex_exit(&(log_sys->mutex));
}

/*将数据库切换到在线备份状态*/
ulint log_switch_backup_state_on(void)
{
	dulint	backup_lsn;

	mutex_enter(&(log_sys->mutex));
	if (log_sys->online_backup_state) {
		mutex_exit(&(log_sys->mutex));

		return(DB_ERROR);
	}

	log_sys->online_backup_state = TRUE;
	backup_lsn = log_sys->lsn;
	log_sys->online_backup_lsn = backup_lsn;

	mutex_exit(&(log_sys->mutex));

	/* log_checkpoint_and_mark_file_spaces(); */

	return(DB_SUCCESS);
}

/*将数据库切换出在线备份状态*/
ulint log_switch_backup_state_off(void)
{
	mutex_enter(&(log_sys->mutex));

	if (!log_sys->online_backup_state) {
		mutex_exit(&(log_sys->mutex));

		return(DB_ERROR);
	}

	log_sys->online_backup_state = FALSE;
	mutex_exit(&(log_sys->mutex));

	return(DB_SUCCESS);
}

/*数据库关闭时，对logs做保存操作*/
void logs_empty_and_mark_files_at_shutdown()
{
	dulint	lsn;
	ulint	arch_log_no;

	ut_print_timestamp(stderr);
	fprintf(stderr, "  InnoDB: Starting shutdown...\n");

	srv_shutdown_state = SRV_SHUTDOWN_CLEANUP;

loop:
	os_thread_sleep(100000);
	mutex_enter(&kernel_mutex);

	if(trx_n_mysql_transactions > 0 || UT_LIST_GET_LEN(trx_sys->trx_list) > 0){
		mutex_exit(&kernel_mutex);
		goto loop;
	}

	/*active线程没有退出*/
	if (srv_n_threads_active[SRV_MASTER] != 0) {
		mutex_exit(&kernel_mutex);
		goto loop;
	}
	mutex_exit(&kernel_mutex);

	mutex_enter(&(log_sys->mutex));
	/*有IO Flush操作正在执行,等待其结束*/
	if(log_sys->n_pending_archive_ios + log_sys->n_pending_checkpoint_writes + log_sys->n_pending_writes > 0){
		mutex_exit(&(log_sys->mutex));
		goto loop;
	}

	mutex_exit(&(log_sys->mutex));
	if(!buf_pool_check_no_pending_io())
		goto loop;

	/*强制log的checkpoint和归档*/
	log_archive_all();
	log_make_checkpoint_at(ut_dulint_max, TRUE);

	mutex_enter(&(log_sys->mutex));
	lsn = log_sys->lsn;
	if(ut_dulint_cmp(lsn, log_sys->last_checkpoint_lsn) != 0 || || (srv_log_archive_on 
		&& ut_dulint_cmp(lsn, ut_dulint_add(log_sys->archived_lsn, LOG_BLOCK_HDR_SIZE)) != 0)){
			mutex_exit(&(log_sys->mutex));
			goto loop;
	}

	arch_log_no = UT_LIST_GET_FIRST(log_sys->log_groups)->archived_file_no;

	if (0 == UT_LIST_GET_FIRST(log_sys->log_groups)->archived_offset)
		arch_log_no--;

	/*等待archive操作完成*/
	log_archive_close_groups(TRUE);

	mutex_exit(&(log_sys->mutex));

	/*表数据刷盘*/
	fil_flush_file_spaces(FIL_TABLESPACE);
	/*log数据刷盘*/
	fil_flush_file_spaces(FIL_LOG);

	if(!buf_all_freed)
		goto loop;

	if (srv_lock_timeout_and_monitor_active)
		goto loop;

	srv_shutdown_state = SRV_SHUTDOWN_LAST_PHASE;

	fil_write_flushed_lsn_to_data_files(lsn, arch_log_no);	
	fil_flush_file_spaces(FIL_TABLESPACE);

	ut_print_timestamp(stderr);
	fprintf(stderr, "  InnoDB: Shutdown completed\n");
}

ibool log_check_log_recs(byte* buf, ulint len, dulint buf_start_lsn)
{
	dulint	contiguous_lsn;
	dulint	scanned_lsn;
	byte*	start;
	byte*	end;
	byte*	buf1;
	byte*	scan_buf;

	ut_ad(mutex_own(&(log_sys->mutex)));

	if (len == 0)
		return(TRUE);


	start = ut_align_down(buf, OS_FILE_LOG_BLOCK_SIZE);
	end = ut_align(buf + len, OS_FILE_LOG_BLOCK_SIZE);

	buf1 = mem_alloc((end - start) + OS_FILE_LOG_BLOCK_SIZE);
	scan_buf = ut_align(buf1, OS_FILE_LOG_BLOCK_SIZE);

	ut_memcpy(scan_buf, start, end - start);
	/*检查记录恢复*/
	recv_scan_log_recs(TRUE, buf_pool_get_curr_size() - RECV_POOL_N_FREE_BLOCKS * UNIV_PAGE_SIZE,	
		FALSE, scan_buf, end - start,
		ut_dulint_align_down(buf_start_lsn,
		OS_FILE_LOG_BLOCK_SIZE),
		&contiguous_lsn, &scanned_lsn);

	ut_a(ut_dulint_cmp(scanned_lsn, ut_dulint_add(buf_start_lsn, len)) == 0);
	ut_a(ut_dulint_cmp(recv_sys->recovered_lsn, scanned_lsn) == 0);

	mem_free(buf1);

	return(TRUE);
}

/*状态信息输出*/
void log_print(char* buf, char*	buf_end)
{
	double	time_elapsed;
	time_t	current_time;

	if (buf_end - buf < 300)
		return;

	mutex_enter(&(log_sys->mutex));

	buf += sprintf(buf, "Log sequence number %lu %lu\n"
		"Log flushed up to   %lu %lu\n"
		"Last checkpoint at  %lu %lu\n",
		ut_dulint_get_high(log_sys->lsn),
		ut_dulint_get_low(log_sys->lsn),
		ut_dulint_get_high(log_sys->written_to_some_lsn),
		ut_dulint_get_low(log_sys->written_to_some_lsn),
		ut_dulint_get_high(log_sys->last_checkpoint_lsn),
		ut_dulint_get_low(log_sys->last_checkpoint_lsn));

	current_time = time(NULL);

	time_elapsed = 0.001 + difftime(current_time,
		log_sys->last_printout_time);
	buf += sprintf(buf,
		"%lu pending log writes, %lu pending chkp writes\n"
		"%lu log i/o's done, %.2f log i/o's/second\n",
		log_sys->n_pending_writes,
		log_sys->n_pending_checkpoint_writes,
		log_sys->n_log_ios,
		(log_sys->n_log_ios - log_sys->n_log_ios_old) / time_elapsed);

	log_sys->n_log_ios_old = log_sys->n_log_ios;
	log_sys->last_printout_time = current_time;

	mutex_exit(&(log_sys->mutex));
}

/*记录单位时间内的io次数和打印的时刻*/
void log_refresh_stats(void)
{
	log_sys->n_log_ios_old = log_sys->n_log_ios;
	log_sys->last_printout_time = time(NULL);
}













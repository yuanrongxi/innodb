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

	/*log存档选项是激活的*/
	if(log->archiving_state != LOG_ARCH_OFF){
		/*计算lsn和archived_lsn的差值*/
		archived_lsn_age = ut_dulint_minus(log->lsn, log->archived_lsn);
		if(archived_lsn_age + len_upper_limit > log->max_archived_lsn_age){ /*正在存档状态又超过存档的lsn 最大范围*/
			mutex_exit(&(log->mutex));

			ut_ad(len_upper_limit <= log->max_archived_lsn_age);
			/*同步进行archive write*/
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
		log_block_init(log_block + OS_FILE_LOG_BLOCK_SIZE, log->lsn);
	}
	else /*更改lsn*/
		log->lsn = ut_dulint_add(log->lsn, len);

	log->buf_free += len;
	ut_ad(log->buf_free <= log->buf_size);

	if(str_len > 0)
		goto part_loop;
}

dulint log_close()
{
	byte*	log_block;
	ulint	first_rec_group;
	dulint	oldest_lsn;
	dulint	lsn;
	log_t*	log	= log_sys;

	ut_ad(mutex_own(&(log->mutex)));

	lsn = log->lsn;

	log_block = ut_align_down(log->buf + log->buf_free, OS_FILE_LOG_BLOCK_SIZE);
	first_rec_group = log_block_get_first_rec_group(log_block);
	if(first_rec_group == 0){
		log_block_set_first_rec_group(log_block, log_block_get_data_len(log_block));
	}

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

/*获得在file里的相对偏移量*/
UNIV_INLINE ulint log_group_calc_real_offset(ulint offset, log_group_t* group)
{
	ut_ad(mutex_own(&(log_sys->mutex)));

	return (offset + LOG_FILE_HDR_SIZE * (1 + offset / (group->file_size - LOG_FILE_HDR_SIZE)));
}

/*返回相对group起始位置的相对位置*/
static ulint log_group_calc_lsn_offset(dulint lsn, log_group_t* group)
{
	dulint	        gr_lsn;
	int64_t			gr_lsn_size_offset;
	int64_t			difference;
	int64_t			group_size;
	int64_t			offset;

	ut_ad(mutex_own(&(log_sys->mutex)));

	gr_lsn = group->lsn;
	/*获得绝对偏移*/
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

	/*返回相对的位置*/
	return log_group_calc_real_offset(offset, group);
}

ulint log_calc_where_lsn_is(int64_t* log_file_offset, dulint first_header_lsn, dulint lsn, ulint n_log_files, int64_t log_file_size)
{
	int64_t	ib_lsn;
	int64_t	ib_first_header_lsn;
	int64_t	capacity = log_file_size - LOG_FILE_HDR_SIZE;
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
	/*获得最新的相对偏移量*/
	group->lsn_offset = log_group_calc_lsn_offset(lsn, group);
	/*设置新的lsn值*/
	group->lsn = lsn;
}

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
		/*重新评估最小的group容量*/
		if(log_group_get_capacity(group) < smallest_capacity)
			smallest_capacity = log_group_get_capacity;

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

	/*为了512字节对齐，所以在开辟内存的时候一定要大于512*/
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

void log_group_init(ulint id, ulint n_files, ulint file_size, ulint space_id, ulint archive_space_id)
{

}













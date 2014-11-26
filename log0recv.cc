#include "log0recv.h"

#include "mem0mem.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "srv0srv.h"
#include "mtr0mtr.h"
#include "mtr0log.h"
#include "page0page.h"
#include "page0cur.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "ibuf0ibuf.h"
#include "trx0undo.h"
#include "trx0rec.h"
#include "trx0roll.h"
#include "btr0cur.h"
#include "btr0cur.h"
#include "btr0cur.h"
#include "dict0boot.h"
#include "fil0fil.h"

#define RECV_DATA_BLOCK_SIZE	(MEM_MAX_ALLOC_IN_BUF - sizeof(recv_data_t))

#define RECV_READ_AHEAD_AREA	32

recv_sys_t*		recv_sys = NULL;
ibool			recv_recovery_on = FALSE;
ibool			recv_recovery_from_backup_on = FALSE;
ibool			recv_needed_recovery = FALSE;

ibool			recv_no_ibuf_operations = FALSE;

ulint			recv_scan_print_counter = 0;

ibool			recv_is_from_backup = FALSE;
ibool			recv_is_making_a_backup = FALSE;

ulint			recv_previous_parsed_rec_type	= 999999;
ulint			recv_previous_parsed_rec_offset	= 0;
ulint			recv_previous_parsed_rec_is_multi = 0;

#define SYS_MUTEX &(recv_sys->mutex)

/***************************************************************/
/*构建一个系统恢复对象recv_sys*/
void recv_sys_create()
{
	if(recv_sys == NULL){
		recv_sys = mem_alloc(sizeof(recv_sys_t));
		
		mutex_create(&(recv_sys->mutex));
		mutex_set_level(&(recv_sys->mutex), SYNC_RECV);

		recv_sys->heap = NULL;
		recv_sys->addr_hash = NULL;
	}
}

/*初始化recv_sys*/
void recv_sys_init(ibool recover_from_backup, ulint available_memory)
{
	if(recv_sys->heap != NULL)
		return ;

	mutex_enter(SYS_MUTEX);
	
	if(!recover_from_backup)
		recv_sys->heap = mem_heap_create_in_buffer(256);
	else{
		recv_sys->heap = mem_heap_create(256);
		recv_is_from_backup = TRUE;
	}

	recv_sys->buf = ut_malloc(RECV_PARSING_BUF_SIZE);
	recv_sys->len = 0;
	recv_sys->recovered_offset = 0;

	/*建立hash table*/
	recv_sys->addr_hash = hash_create(available_memory / 64);
	recv_sys->n_addrs = 0;
	
	recv_sys->apply_log_recs = FALSE;
	recv_sys->apply_batch_on = FALSE;

	recv_sys->last_block_buf_start = mem_alloc(2 * OS_FILE_LOG_BLOCK_SIZE);
	recv_sys->last_block = ut_align(recv_sys->last_block_buf_start, OS_FILE_LOG_BLOCK_SIZE);
	recv_sys->found_corrupt_log = FALSE;

	mutex_exit(SYS_MUTEX);
}

static void recv_sys_empty_hash()
{
	ut_ad(mutex_own(SYS_MUTEX));
	ut_a(recv_sys->n_addrs == 0);

	/*清空hash table和heap*/
	hash_table_free(recv_sys->addr_hash);
	mem_heap_empty(recv_sys->heap);

	/*重新建立一个addr_hash*/
	recv_sys->addr_hash = hash_create(buf_pool_get_curr_size() / 256);
}

/*释放recv_sys对象*/
void recv_sys_free()
{
	mutex_enter(SYS_MUTEX);
	
	hash_table_free(recv_sys->addr_hash);
	mem_heap_free(recv_sys->heap);
	ut_free(recv_sys->buf);
	mem_free(recv_sys->last_block_buf_start);

	recv_sys->addr_hash = NULL;
	recv_sys->heap = NULL;

	mutex_exit(SYS_MUTEX);
}

static void recv_truncate_group(log_group_t* group, dulint recovered_lsn, dulint limit_lsn, dulint checkpoint_lsn, dulint archived_lsn)
{
	dulint	start_lsn;
	dulint	end_lsn;
	dulint	finish_lsn1;
	dulint	finish_lsn2;
	dulint	finish_lsn;
	ulint	len;
	ulint	i;

	/*无archive file模式*/
	if(ut_dulint_cmp(archived_lsn, ut_dulint_max) == 0)
		archived_lsn = checkpoint_lsn;

	finish_lsn1 = ut_dulint_add(ut_dulint_align_down(archived_lsn, OS_FILE_LOG_BLOCK_SIZE), log_group_get_capacity(group));
	finish_lsn2 = ut_dulint_add(ut_dulint_align_up(recovered_lsn, OS_FILE_LOG_BLOCK_SIZE), recv_sys->last_log_buf_size);

	if(ut_dulint_cmp(limit_lsn, ut_dulint_max) != 0)
		finish_lsn = finish_lsn1;
	else
		finish_lsn = ut_dulint_get_min(finish_lsn1, finish_lsn2);

	ut_a(RECV_SCAN_SIZE <= log_sys->buf_size);
	for(i = 0; i < RECV_SCAN_SIZE; i ++)
		*(log_sys->buf + i) = 0;

	start_lsn = ut_dulint_align_down(recovered_lsn, OS_FILE_LOG_BLOCK_SIZE);

	if(ut_dulint_cmp(start_lsn, recovered_lsn) != 0) {
		ut_memcpy(log_sys->buf, recv_sys->last_block, OS_FILE_LOG_BLOCK_SIZE);
		log_block_set_data_len(log_sys->buf, ut_dulint_minus(recovered_lsn, start_lsn));
	}

	if(ut_dulint_cmp(start_lsn, finish_lsn) >= 0)
		return ;

	for(;;){
		end_lsn = ut_dulint_add(start_lsn, RECV_SCAN_SIZE);
		if (ut_dulint_cmp(end_lsn, finish_lsn) > 0) 
			end_lsn = finish_lsn;

		len = ut_dulint_minus(end_lsn, start_lsn);
		log_group_write_buf(LOG_RECOVER, group, log_sys->buf, len, start_lsn, 0);
		if(ut_dulint_cmp(end_lsn, finish_lsn) >= 0)
			return ;

		for(i = 0; i < RECV_SCAN_SIZE; i ++)
			*(log_sys->buf + i) = 0;

		start_lsn = end_lsn;
	}
}

static void recv_copy_group(log_group_t* up_to_date_group, log_group_t* group, dulint recovered_lsn)
{
	dulint	start_lsn;
	dulint	end_lsn;
	ulint	len;

	if(ut_dulint_cmp(group->scanned_lsn, recovered_lsn) >= 0)
		return ;

	ut_a(RECV_SCAN_SIZE <= log_sys->buf_size);
	start_lsn = ut_dulint_align_down(group->scanned_lsn, OS_FILE_LOG_BLOCK_SIZE);
	for(;;){
		end_lsn = ut_dulint_add(start_lsn, RECV_SCAN_SIZE);
		if (ut_dulint_cmp(end_lsn, recovered_lsn) > 0)
			end_lsn = ut_dulint_align_up(recovered_lsn, OS_FILE_LOG_BLOCK_SIZE);
		/*从up_to_date_group读取一个日志seg*/
		log_group_read_log_seg(LOG_RECOVER, log_sys->buf, up_to_date_group, start_lsn, end_lsn);
		
		/*将读出的日志写入到group当中*/
		len = ut_dulint_minus(end_lsn, start_len);
		log_group_write_buf(LOG_RECOVER, group, log_sys->buf, len, start_lsn, 0);

		if(ut_dulint_cmp(end_lsn, recovered_lsn) >= 0)
			return ;

		start_lsn = end_lsn;
	}
}



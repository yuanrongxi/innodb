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
/*将up_to_data_group中的日志拷贝到group当中*/
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

/*将up_to_date_group中的日志同步到其的group当中*/
void recv_synchronize_groups(log_group_t* up_to_date_group)
{
	log_group_t*	group;
	dulint			start_lsn;
	dulint			end_lsn;
	dulint			recovered_lsn;
	dulint			limit_lsn;

	recovered_lsn = recv_sys->recovered_lsn;
	limit_lsn = recv_sys->limit_lsn;

	start_lsn = ut_dulint_align_down(recovered_lsn, OS_FILE_LOG_BLOCK_SIZE);
	end_lsn = ut_dulint_align_up(recovered_lsn, OS_FILE_LOG_BLOCK_SIZE);

	ut_a(ut_dulint_cmp(start_lsn, end_lsn) != 0);
	/*从up_to_date_group中读取一个日志片段到last_block中*/
	log_group_read_log_seg(LOG_RECOVER, recv_sys->last_block, up_to_date_group, start_lsn, end_lsn);

	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	while(group){
		if(group != up_to_date_group) /*做group 日志复制*/
			recv_copy_group(group, up_to_date_group, recovered_lsn);

		/*设置新的LSN*/
		log_group_set_fields(group, recovered_lsn);
		group = UT_LIST_GET_NEXT(log_groups, group);
	}
	/*为日志建立一个checkpoint*/
	log_groups_write_checkpoint_info();

	mutex_exit(&log_sys->mutex);

	/*释放log_sys->mutex,等待建立checkpoint完成*/
	rw_lock_s_lock(&(log_sys->checkpoint_lock));
	rw_lock_s_unlock(&(log_sys->checkpoint_lock));
	
	mutex_enter(&(log_sys->mutex));
}

/*检查checkpoint的合法性*/
static ibool recv_check_cp_is_consistent(byte* buf)
{
	ulint fold;
	/*计算第一个校验值*/
	fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);
	if((fold & 0xFFFFFFFF) != mach_read_from_4(buf, + LOG_CHECKPOINT_CHECKSUM_1))
		return FALSE;

	/*计算第二个校验值*/
	fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);
	if ((fold & 0xFFFFFFFF) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2))
			return FALSE;

	return TRUE;
}

/*在groups中查找LSN最大和法的checkpoint*/
static ulint recv_find_max_checkpoint(log_group_t** max_group, ulint* max_field)
{
	log_group_t*	group;
	dulint		max_no;
	dulint		checkpoint_no;
	ulint		field;
	byte*		buf;

	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	max_no = ut_dulint_zero;
	*max_group = NULL;

	buf = log_sys->checkpoint_buf;
	while(group){
		group->state = LOG_GROUP_CORRUPTED;

		for(field = LOG_CHECKPOINT_1; field <= LOG_CHECKPOINT_2; field += LOG_CHECKPOINT_2 - LOG_CHECKPOINT_1){
			/*从group的文件中读取一个checkpoint信息到group->checkpoint_buf里面*/
			log_group_read_checkpoint_info(group, field);
			
			if(!recv_check_cp_is_consistent(buf)){ /*校验checkpoint信息的合法性*/
				if (log_debug_writes)
					fprintf(stderr, "InnoDB: Checkpoint in group %lu at %lu invalid, %lu\n",
						group->id, field, mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1));

				goto not_consistent;
			}

			/*从checkpoint buf中读取对应的checkpoint信息*/
			group->state = LOG_GROUP_OK;
			group->lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
			group->lsn_offset = mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET);
			checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

			if(log_debug_writes)
				fprintf(stderr, "InnoDB: Checkpoint number %lu found in group %lu\n", ut_dulint_get_low(checkpoint_no), group->id);

			if(ut_dulint_cmp(checkpoint_no, max_no)){ /*比较checkpoint no序号*/
				*max_group = group;
				*max_field = field;
				max_no = checkpoint_no;
			}
not_consistent:
			;
		}

		group = UT_LIST_GET_NEXT(log_groups, group);
	}

	if(*max_group == NULL){
		fprintf(stderr,
			"InnoDB: No valid checkpoint found.\n"
			"InnoDB: If this error appears when you are creating an InnoDB database,\n"
			"InnoDB: the problem may be that during an earlier attempt you managed\n"
			"InnoDB: to create the InnoDB data files, but log file creation failed.\n"
			"InnoDB: If that is the case, please refer to section 3.1 of\n"
			"InnoDB: http://www.innodb.com/ibman.html\n");

		return DB_ERROR;
	}

	return DB_SUCCESS;
}

ibool recv_cp_info_for_backup(byte* hdr, dulint* lsn, ulint* offset, ulint* fsp_limit, dulint* cp_no, dulint* first_header_lsn)
{
	ulint	max_cp		= 0;
	dulint	max_cp_no	= ut_dulint_zero;
	byte*	cp_buf;

	cp_buf = hdr + LOG_CHECKPOINT_1;
	if(recv_check_cp_is_consistent(cp_buf)){
		max_cp_no = mach_read_from_8(cp_buf + LOG_CHECKPOINT_NO);
		max_cp = LOG_CHECKPOINT_1;
	}

	cp_buf = hdr + LOG_CHECKPOINT_2;
	if (recv_check_cp_is_consistent(cp_buf)) {
		if (ut_dulint_cmp(mach_read_from_8(cp_buf + LOG_CHECKPOINT_NO), max_cp_no) > 0) /*比LOG_CHECKPOINT_1中的序号大,将其设置为真正的checkpoint*/
				max_cp = LOG_CHECKPOINT_2;
	}

	if(max_cp == 0)
		return FALSE;

	/*将选定的检查点信息从BUF中读取出来*/
	cp_buf = hdr + max_cp;
	*lsn = mach_read_from_8(cp_buf + LOG_CHECKPOINT_LSN);
	*offset = mach_read_from_4(cp_buf + LOG_CHECKPOINT_OFFSET);
	/*对checkpoint魔法字进行校验*/
	if (mach_read_from_4(cp_buf + LOG_CHECKPOINT_FSP_MAGIC_N) == LOG_CHECKPOINT_FSP_MAGIC_N_VAL) {
		*fsp_limit = mach_read_from_4(cp_buf + LOG_CHECKPOINT_FSP_FREE_LIMIT);
		if (*fsp_limit == 0) *fsp_limit = 1000000000;
	} 
	else
		*fsp_limit = 1000000000;

	*cp_no = mach_read_from_8(cp_buf + LOG_CHECKPOINT_NO);
	*first_header_lsn = mach_read_from_8(hdr + LOG_FILE_START_LSN);

	return TRUE;
}

/*校验新老块格式的block合法性*/
static ibool log_block_checksum_is_ok_old_format(byte* block)
{
	/*新格式*/
	if (log_block_calc_checksum(block) == log_block_get_checksum(block))
		return TRUE;
	/*老格式*/
	if(log_block_get_hdr_no(block) == log_block_get_checksum(block))
		return TRUE;

	return FALSE;
}

/*扫描一段log片段，返回有效block的n_byte_scanned长度和scanned_checkpoint_no*/
void recv_scan_log_seg_for_backup(byte* buf, ulint buf_len, dulint* scanned_lsn, ulint* scanned_checkpoint_no, ulint* n_byte_scanned)
{
	ulint data_len;
	byte* log_block;
	ulint no;

	*n_byte_scanned = 0;

	for(log_block = buf; log_block < buf + buf_len; log_block += OS_FILE_LOG_BLOCK_SIZE){
		no = log_block_get_hdr_no(log_block);
		/*block no与scanned_lsn不相关联或者block不合法*/
		if(no != log_block_convert_lsn_to_no(*scanned_lsn) || !log_block_checksum_is_ok_old_format(log_block)){
			log_block += OS_FILE_LOG_BLOCK_SIZE;
			break;
		}

		/*buf可能是最近log buffer刷盘产生的无效数据*/
		if(*scanned_checkpoint_no > 0 && log_block_get_checkpoint_no(log_block) < *scanned_checkpoint_no
			&& *scanned_checkpoint_no - log_block_get_checkpoint_no(log_block) > 0x80000000)
			break;
		
		data_len = log_block_get_data_len(log_block);
		*scanned_checkpoint_no = log_block_get_checkpoint_no(log_block);
		*scanned_lsn = ut_dulint_add(*scanned_lsn, data_len);

		*n_byte_scanned = data_len;
		if(data_len < OS_FILE_LOG_BLOCK_SIZE)
			break;
	}
}

static byte* recv_parse_or_apply_log_rec_body(byte type, byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	byte* new_ptr;

	/*进行各种mtr操作调用*/
	if(type <= MLOG_8BYTES)
		new_ptr = mlog_parse_nbytes(type, ptr, end_ptr, page);
	else if (type == MLOG_REC_INSERT)
		new_ptr = page_cur_parse_insert_rec(FALSE, ptr, end_ptr, page,mtr);
	else if (type == MLOG_REC_CLUST_DELETE_MARK)
		new_ptr = btr_cur_parse_del_mark_set_clust_rec(ptr, end_ptr,page);
	else if (type == MLOG_REC_SEC_DELETE_MARK)
		new_ptr = btr_cur_parse_del_mark_set_sec_rec(ptr, end_ptr, page);
	else if (type == MLOG_REC_UPDATE_IN_PLACE)
		new_ptr = btr_cur_parse_update_in_place(ptr, end_ptr, page);
	else if ((type == MLOG_LIST_END_DELETE) || (type == MLOG_LIST_START_DELETE))
		new_ptr = page_parse_delete_rec_list(type, ptr, end_ptr, page, mtr);
	else if (type == MLOG_LIST_END_COPY_CREATED)
		new_ptr = page_parse_copy_rec_list_to_created_page(ptr, end_ptr, page, mtr);
	else if (type == MLOG_PAGE_REORGANIZE)
		new_ptr = btr_parse_page_reorganize(ptr, end_ptr, page, mtr);
	else if (type == MLOG_PAGE_CREATE)
		new_ptr = page_parse_create(ptr, end_ptr, page, mtr);
	else if (type == MLOG_UNDO_INSERT)
		new_ptr = trx_undo_parse_add_undo_rec(ptr, end_ptr, page);
	else if (type == MLOG_UNDO_ERASE_END)
		new_ptr = trx_undo_parse_erase_page_end(ptr, end_ptr, page, mtr);
	else if (type == MLOG_UNDO_INIT)
		new_ptr = trx_undo_parse_page_init(ptr, end_ptr, page, mtr);
	else if (type == MLOG_UNDO_HDR_DISCARD)
		new_ptr = trx_undo_parse_discard_latest(ptr, end_ptr, page, mtr);
	else if ((type == MLOG_UNDO_HDR_CREATE) || (type == MLOG_UNDO_HDR_REUSE))
		new_ptr = trx_undo_parse_page_header(type, ptr, end_ptr, page, mtr);
	else if (type == MLOG_REC_MIN_MARK)
		new_ptr = btr_parse_set_min_rec_mark(ptr, end_ptr, page, mtr);
	else if (type == MLOG_REC_DELETE)
		new_ptr = page_cur_parse_delete_rec(ptr, end_ptr, page, mtr);
	else if (type == MLOG_IBUF_BITMAP_INIT)
		new_ptr = ibuf_parse_bitmap_init(ptr, end_ptr, page, mtr);
	else if (type == MLOG_FULL_PAGE)
		new_ptr = mtr_log_parse_full_page(ptr, end_ptr, page);
	else if (type == MLOG_INIT_FILE_PAGE)
		new_ptr = fsp_parse_init_file_page(ptr, end_ptr, page);
	else if (type <= MLOG_WRITE_STRING)
		new_ptr = mlog_parse_string(ptr, end_ptr, page);
	else{
		new_ptr = NULL;
		recv_sys->found_corrupt_log = TRUE;
	}

	ut_ad(!page || new_ptr);

	return new_ptr;
}

UNIV_INLINE ulint recv_fold(ulint space, ulint page_no)
{
	return ut_fold_ulint_pair(space, page_no);
}

/*通过space 和page no在addr_hash中查找recv_addr*/
static recv_addr_t* recv_get_fil_add_struct(ulint space, ulint page_no)
{
	recv_addr_t*  recv_addr;
	recv_addr = HASH_GET_FIRST(recv_sys->addr_hash, recv_hash(space, page_no));
	while(recv_addr){
		/*相同的space和page no*/
		if(recv_addr->space = space && recv_addr->page_no == page_no)
			break;

		recv_addr = HASH_GET_NEXT(addr_hash, recv_addr);
	}

	return recv_addr;
}





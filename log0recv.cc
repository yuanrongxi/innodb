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

/*在groups中查找LSN最大的checkpoint*/
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

ibool recv_read_cp_info_for_backup(byte* hdr, dulint* lsn, ulint* offset, ulint* fsp_limit, dulint* cp_no, dulint* first_header_lsn)
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
static recv_addr_t* recv_get_fil_addr_struct(ulint space, ulint page_no)
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

static void recv_add_to_hash_table(byte type, ulint space, ulint page_no, byte* body, byte* rec_end, dulint start_lsn, dulint end_lsn)
{
	recv_t*		recv;
	ulint		len;
	recv_data_t*	recv_data;
	recv_data_t**	prev_field;
	recv_addr_t*	recv_addr;

	ut_a(space == 0); 

	len = rec_end = body;

	recv = mem_heap_alloc(recv_sys->heap, sizeof(recv_t));
	recv->type = type;
	recv->len = rec_end - body;
	recv->start_lsn = end_lsn;

	/*hash table中没有这个单元，新建一个recv_addr,并将recv加入到其中*/
	recv_addr = recv_get_fil_add_struct(space, page_no);
	if(recv_addr == NULL){
		recv_addr = mem_heap_alloc(recv_sys->heap, sizeof(recv_addr_t));
		recv_addr->space = space;
		recv_addr->page_no = page_no;
		recv_addr->state = RECV_NOT_PROCESSED;

		UT_LIST_INIT(recv_addr->rec_list);
		HASH_INSERT(recv_addr_t, addr_hash, recv_sys->addr_hash, recv_fold(space, page_no), recv_addr);
	}

	UT_LIST_ADD_LAST(rec_list, recv_addr->rec_list, recv);
	prev_field = &(recv->data);

	while(rec_end > body){
		len = rec_end - body;
		if(len > RECV_DATA_BLOCK_SIZE)
			len = RECV_DATA_BLOCK_SIZE;

		recv_data = mem_heap_alloc(recv_sys->heap, sizeof(recv_data_t) + len);
		*prev_field = recv_data;
		/*将数据填补到recv_data的后面空间上*/
		ut_memcpy(((byte*)recv_data) + sizeof(recv_data_t), body, len);

		prev_field = &(recv_data->next);
		body += len;
	}

	*prev_field = NULL;
}

/*从recv中将记录数据读取到buf当中*/
static void recv_data_copy_to_buf(byte* buf, recv_t* recv)
{
	recv_data_t*	recv_data;
	ulint		part_len;
	ulint		len;

	len = recv->len;
	recv_data = recv->data;
	while(len > 0){
		if(len > RECV_DATA_BLOCK_SIZE)
			part_len = RECV_DATA_BLOCK_SIZE;
		else
			part_len = len;

		ut_memcpy(buf, ((byte*)recv_data) + sizeof(recv_data_t), part_len);
		buf += part_len;
		len -= part_len;

		recv_data = recv_data->next;
	}
}

/*当page的LSN小于日志记录的LSN,将hash log中的记录写入到page当中*/
void recv_recover_page(ibool recover_backup, ibool just_read_in, page_t* page, ulint space, ulint page_no)
{
	buf_block_t*	block;
	recv_addr_t*	recv_addr;
	recv_t*		recv;
	byte*		buf;
	dulint		start_lsn;
	dulint		end_lsn;
	dulint		page_lsn;
	dulint		page_newest_lsn;
	ibool		modification_to_page;
	ibool		success;
	mtr_t		mtr;

	mutex_enter(SYS_MUTEX);

	if(recv_sys->apply_log_recs == FALSE){
		mutex_exit(SYS_MUTEX);
		return;
	}

	/*获得recv地址*/
	recv_addr = recv_get_fil_addr_struct(space, page_no);
	if(recv_addr == NULL || recv_addr->state == RECV_BEING_PROCESSED || recv_addr->state == RECV_PROCESSED){ /*recv_addr已经开始处理或者已经处理了*/
		mutex_exit(SYS_MUTEX);
		return;
	}

	/*更改recv_addr的状态，变成开始处理的状态*/
	recv_addr->state = RECV_BEING_PROCESSED;
	mutex_exit(SYS_MUTEX);
	
	/*开始以一个mtr*/
	mtr_start(&mtr);
	mtr_set_log_mode(&mtr, MTR_LOG_NONE);
	if(!recover_backup){
		block = buf_block_align(page);
		if(just_read_in) /*转移block->lock的归属线程*/
			rw_lock_x_lock_move_ownership(&(block->lock));

		success = buf_page_get_known_nowait(RW_X_LATCH, page, BUF_KEEP_OLD, IB__FILE__, __LINE__, &mtr);
		ut_a(success);

		buf_page_dbg_add_level(page, SYNC_NO_ORDER_CHECK);
	}

	/*获得page的lsn*/
	page_lsn = mach_read_from_8(page + FIL_PAGE_LSN);
	if (!recover_backup) {
		page_newest_lsn = buf_frame_get_newest_modification(page);
		if(!ut_dulint_is_zero(page_newest_lsn))
			page_lsn = page_newest_lsn;
	}
	else
		page_newest_lsn = ut_dulint_zero;

	modification_to_page = FALSE;

	recv = UT_LIST_GET_FIRST(recv_addr->rec_list);
	while(recv){
		end_lsn = recv->end_lsn;
		/*从recv拷贝数据*/
		if(recv->end_lsn > RECV_DATA_BLOCK_SIZE){
			buf = mem_alloc(recv->len);
			recv_data_copy_to_buf(buf, recv);
		}
		else
			buf = ((byte*)recv->data) + sizeof(recv_data_t);

		if(recv->type == MLOG_INIT_FILE_PAGE || recv->type == MLOG_FULL_PAGE){
			page_lsn = page_newest_lsn;
			mach_write_to_8(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN, ut_dulint_zero);
			mach_write_to_8(page + FIL_PAGE_LSN, ut_dulint_zero);
		}

		if (ut_dulint_cmp(recv->start_lsn, page_lsn) >= 0) {
			if (!modification_to_page) {
				modification_to_page = TRUE;
				start_lsn = recv->start_lsn;
			}

			if (log_debug_writes)
				fprintf(stderr, "InnoDB: Applying log rec type %lu len %lu to space %lu page no %lu\n",
					(ulint)recv->type, recv->len, recv_addr->space, recv_addr->page_no);

			recv_parse_or_apply_log_rec_body(recv->type, buf, buf + recv->len, page, &mtr);
			mach_write_to_8(page + UNIV_PAGE_SIZE - FIL_PAGE_END_LSN, ut_dulint_add(recv->start_lsn, recv->len));
			mach_write_to_8(page + FIL_PAGE_LSN, ut_dulint_add(recv->start_lsn, recv->len));
		}

		if (recv->len > RECV_DATA_BLOCK_SIZE)
			mem_free(buf);

		recv = UT_LIST_GET_NEXT(rec_list, recv);
	}

	mutex_enter(SYS_MUTEX);
	recv_addr->state = RECV_PROCESSED;

	ut_a(recv_sys->n_addrs);
	recv_sys->n_addrs --;
	mutex_exit(SYS_MUTEX);

	if(!recover_backup && modification_to_page)
		buf_flush_recv_note_modification(block, start_lsn, end_lsn);

	mtr.modifications = FALSE;
	mtr_commit(&mtr);
}

static ulint recv_read_in_area(ulint space, ulint page_no)
{
	recv_addr_t* recv_addr;
	ulint	page_nos[RECV_READ_AHEAD_AREA];
	ulint	low_limit;
	ulint	n;

	low_limit = page_no - (page_no % RECV_READ_AHEAD_AREA);
	n = 0;

	for (page_no = low_limit; page_no < low_limit + RECV_READ_AHEAD_AREA; page_no++) {
		/*通过space和page_no获得recv_addr*/
		recv_addr = recv_get_fil_addr_struct(space, page_no);
		if (recv_addr && !buf_page_peek(space, page_no)) {
			mutex_enter(&(recv_sys->mutex));

			if (recv_addr->state == RECV_NOT_PROCESSED) { /*找到RECV_NOT_PROCESSED状态的recv_addr*/
				recv_addr->state = RECV_BEING_READ;
	
				page_nos[n] = page_no;
				n++;
			}
			
			mutex_exit(&(recv_sys->mutex));
		}
	}

	buf_read_recv_pages(FALSE, space, page_nos, n);

	return(n);
}

void recv_apply_hashed_log_recs(ibool allow_ibuf)
{
	recv_addr_t* recv_addr;
	page_t*	page;
	ulint	i;
	ulint	space;
	ulint	page_no;
	ulint	n_pages;
	ibool	has_printed	= FALSE;
	mtr_t	mtr;

loop:
	mutex_enter(&(recv_sys->mutex));

	if (recv_sys->apply_batch_on) {
		mutex_exit(&(recv_sys->mutex));
		os_thread_sleep(500000);
		goto loop;
	}

	if (!allow_ibuf) {
		ut_ad(mutex_own(&(log_sys->mutex)));
		recv_no_ibuf_operations = TRUE;
	} 
	else
		ut_ad(!mutex_own(&(log_sys->mutex)));

	/*标识正在批量应用日志*/
	recv_sys->apply_log_recs = TRUE;
	recv_sys->apply_batch_on = TRUE;

	for (i = 0; i < hash_get_n_cells(recv_sys->addr_hash); i++) {
		recv_addr = HASH_GET_FIRST(recv_sys->addr_hash, i);

		while (recv_addr) {
			space = recv_addr->space;
			page_no = recv_addr->page_no;

			if (recv_addr->state == RECV_NOT_PROCESSED) {
				if (!has_printed) {
					ut_print_timestamp(stderr);
					fprintf(stderr, 
						"  InnoDB: Starting an apply batch of log records to the database...\n"
						"InnoDB: Progress in percents: ");
					has_printed = TRUE;
				}
				
				mutex_exit(&(recv_sys->mutex));

				if (buf_page_peek(space, page_no)) {
					mtr_start(&mtr);
					page = buf_page_get(space, page_no, RW_X_LATCH, &mtr);

					buf_page_dbg_add_level(page, SYNC_NO_ORDER_CHECK);
					/*将recv_addr_t上的log应用到对应的页上*/
					recv_recover_page(FALSE, FALSE, page, space, page_no);

					mtr_commit(&mtr);
				} 
				else
					recv_read_in_area(space, page_no);

				mutex_enter(&(recv_sys->mutex));
			}

			recv_addr = HASH_GET_NEXT(addr_hash, recv_addr);
		}

		if (has_printed && (i * 100) / hash_get_n_cells(recv_sys->addr_hash) != ((i + 1) * 100) / hash_get_n_cells(recv_sys->addr_hash))
			fprintf(stderr, "%lu ", (i * 100) / hash_get_n_cells(recv_sys->addr_hash));

	}
	/* Wait until all the pages have been processed */
	while (recv_sys->n_addrs != 0) {
		mutex_exit(&(recv_sys->mutex));
		os_thread_sleep(500000);
		mutex_enter(&(recv_sys->mutex));
	}	

	if (has_printed)
	        fprintf(stderr, "\n");

	if (!allow_ibuf) {
		/* Flush all the file pages to disk and invalidate them in
		the buffer pool */

		mutex_exit(&(recv_sys->mutex));
		mutex_exit(&(log_sys->mutex));

		n_pages = buf_flush_batch(BUF_FLUSH_LIST, ULINT_MAX,
								ut_dulint_max);
		ut_a(n_pages != ULINT_UNDEFINED);
		
		buf_flush_wait_batch_end(BUF_FLUSH_LIST);

		buf_pool_invalidate();

		mutex_enter(&(log_sys->mutex));
		mutex_enter(&(recv_sys->mutex));

		recv_no_ibuf_operations = FALSE;
	}

	recv_sys->apply_log_recs = FALSE;
	recv_sys->apply_batch_on = FALSE;
			
	recv_sys_empty_hash();

	if (has_printed)
		fprintf(stderr, "InnoDB: Apply batch completed\n");

	mutex_exit(&(recv_sys->mutex));
}

void recv_apply_log_recs_for_backup(ulint n_data_files, char** data_files, ulint* file_sizes)
{
	recv_addr_t*	recv_addr;
	os_file_t	data_file;
	ulint		n_pages_total	= 0;
	ulint		nth_file	= 0;
	ulint		nth_page_in_file= 0;
	byte*		page;
	ibool		success;
	ulint		i;

	recv_sys->apply_log_recs = TRUE;
	recv_sys->apply_batch_on = TRUE;

	page = buf_pool->frame_zero;

	/*计算page总数*/
	for(i = 0; i < n_data_files; i ++)
		n_pages_total += file_sizes[i];

	printf("InnoDB: Starting an apply batch of log records to the database...\n"
		"InnoDB: Progress in percents: ");

	for(i = 0; i < n_pages_total; i ++){
		if(i == 0 || nth_page_in_file == file_sizes[nth_file]){
			if(i != 0){
				nth_file++;
				nth_page_in_file = 0;
				os_file_flush(data_file);
				os_file_close(data_file);
			}

			data_file = os_file_create_simple(data_files[nth_file], OS_FILE_OPEN, OS_FILE_READ_WRITE, success);
			if(!success){
				printf("InnoDB: Error: cannot open %lu'th data file %s\n", nth_file);
				exit(1);
			}
		}

		/*获得recv_addr*/
		recv_addr = recv_get_fil_addr_struct(0, i);
		if(recv_addr != NULL){
			/*从文件中读取一个page缓冲区数据*/
			success = os_file_read(data_file, page, (nth_page_in_file << UNIV_PAGE_SIZE_SHIFT)
				& 0xFFFFFFFF, nth_page_in_file >> (32 - UNIV_PAGE_SIZE_SHIFT), UNIV_PAGE_SIZE);
			if(!success){
				printf("InnoDB: Error: cannot write page no %lu to %lu'th data file %s\n",nth_page_in_file, nth_file);
				exit(1);
			}

			buf_page_init_for_backup_restore(0, i, buf_block_align(page));
			/*将recv_addr上的log应用到对应的页上*/
			recv_recover_page(TRUE, FALSE, page, 0, i);

			buf_flush_init_for_writing(page, mach_read_from_8(page + FIL_PAGE_LSN), 0, i);

			success = os_file_write(data_files[nth_file],
				data_file, page,
				(nth_page_in_file << UNIV_PAGE_SIZE_SHIFT) & 0xFFFFFFFF,
				nth_page_in_file >> (32 - UNIV_PAGE_SIZE_SHIFT), 
				UNIV_PAGE_SIZE);
		}

		if((100 * i) / n_pages_total != (100 * (i + 1)) / n_pages_total){
			printf("%lu ", (100 * i) / n_pages_total);
			fflush(stdout);
		}

		nth_page_in_file++;
	}

	os_file_flush(data_file);
	os_file_close(data_file);
	/*清空日志恢复的HASH TABLE*/
	recv_sys_empty_hash();
}

static void recv_update_replicate(byte type, ulint space, ulint page_no, byte* body, byte* end_ptr)
{
	page_t*	replica;
	mtr_t	mtr;
	byte*	ptr;

	mtr_start(&mtr);
	mtr_set_log_mode(&mtr, MTR_LOG_NONE);

	replica = buf_page_get(space + RECV_REPLICA_SPACE_ADD, page_no, RW_X_LATCH, &mtr);
	buf_page_dbg_add_level(replica, SYNC_NO_ORDER_CHECK);

	ptr = recv_parse_or_apply_log_rec_body(type, body, end_ptr, replica, &mtr);
	ut_a(ptr == end_ptr);

	buf_flush_recv_note_modification(buf_block_align(replica), log_sys->old_lsn, log_sys->old_lsn);

	mtr.modifications = FASLE;

	mtr_commit(&mtr);
}

static void recv_check_identical(byte* str1, byte* str2, ulint len)
{
	ulint i;
	for(i = 0; i < len; i ++){
		if (str1[i] != str2[i]) {
			fprintf(stderr, "Strings do not match at offset %lu\n", i);

			ut_print_buf(str1 + i, 16);
			fprintf(stderr, "\n");
			ut_print_buf(str2 + i, 16);

			ut_error;
		}
	}
}

static void recv_compare_relicate(ulint page, ulint page_no)
{
	page_t*	replica;
	page_t*	page;
	mtr_t	mtr;

	mtr_start(&mtr);

	mutex_enter(&(buf_pool->mutex));
	page = buf_page_hash_get(space, page_no)->frame;
	mutex_exit(&(buf_pool->mutex));

	replica = buf_page_get(space + RECV_REPLICA_SPACE_ADD, page_no, RW_X_LATCH, &mtr);
	buf_page_dbg_add_level(replica, SYNC_NO_ORDER_CHECK);

	recv_check_identical(page + FIL_PAGE_DATA, replica + FIL_PAGE_DATA, PAGE_HEADER + PAGE_MAX_TRX_ID - FIL_PAGE_DATA);

	recv_check_identical(page + PAGE_HEADER + PAGE_MAX_TRX_ID + 8,
		replica + PAGE_HEADER + PAGE_MAX_TRX_ID + 8,
		UNIV_PAGE_SIZE - FIL_PAGE_DATA_END - PAGE_HEADER - PAGE_MAX_TRX_ID - 8);

	mtr_commit(&mtr);
}


void recv_compare_spaces(ulint space1, ulint space2, ulint n_pages)
{
	page_t*	replica;
	page_t*	page;
	mtr_t	mtr;
	page_t*	frame;
	ulint	page_no;

	replica = buf_frame_alloc();
	page = buf_frame_alloc();

	for (page_no = 0; page_no < n_pages; page_no++) {
		mtr_start(&mtr);

		frame = buf_page_get_gen(space1, page_no, RW_S_LATCH, NULL,
			BUF_GET_IF_IN_POOL,
			IB__FILE__, __LINE__,
			&mtr);
		if (frame) {
			buf_page_dbg_add_level(frame, SYNC_NO_ORDER_CHECK);
			ut_memcpy(page, frame, UNIV_PAGE_SIZE);
		} else {
			/* Read it from file */
			fil_io(OS_FILE_READ, TRUE, space1, page_no, 0, UNIV_PAGE_SIZE, page, NULL);
		}

		frame = buf_page_get_gen(space2, page_no, RW_S_LATCH, NULL, BUF_GET_IF_IN_POOL, IB__FILE__, __LINE__, &mtr);
		if (frame) {
			buf_page_dbg_add_level(frame, SYNC_NO_ORDER_CHECK);
			ut_memcpy(replica, frame, UNIV_PAGE_SIZE);
		} else {
			/* Read it from file */
			fil_io(OS_FILE_READ, TRUE, space2, page_no, 0, UNIV_PAGE_SIZE, replica, NULL);
		}

		recv_check_identical(page + FIL_PAGE_DATA, replica + FIL_PAGE_DATA, PAGE_HEADER + PAGE_MAX_TRX_ID - FIL_PAGE_DATA);

		recv_check_identical(page + PAGE_HEADER + PAGE_MAX_TRX_ID + 8,
			replica + PAGE_HEADER + PAGE_MAX_TRX_ID + 8,
			UNIV_PAGE_SIZE - FIL_PAGE_DATA_END
			- PAGE_HEADER - PAGE_MAX_TRX_ID - 8);

		mtr_commit(&mtr);
	}

	buf_frame_free(replica);
	buf_frame_free(page);
}

void recv_compare_spaces_low(ulint space1, ulint space2, ulint n_pages)
{
	mutex_enter(&(log_sys->mutex));
	/*将hash中的recv_addr_t的日志应用到page当中*/
	recv_apply_hashed_log_recs(FALSE);

	mutex_exit(&(log_sys->mutex));
	recv_compare_spaces(space1, space2, n_pages);
}

/*通过type执行mtr方法调用,日志解析成记录*/
static ulint recv_parse_log_rec(byte* ptr, byte* end_ptr, byte* type, ulint space, ulint* page_no, byte** body)
{
	byte* new_ptr;

	if(ptr == end_ptr)
		return 0;

	if(*ptr == MLOG_MULTI_REC_END){
		*type = *ptr;
		return 1;
	}

	if(*ptr == MLOG_DUMMY_RECORD){
		*type = *ptr;
		*space = 1000;
		return 1;
	}

	new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space, page_no);
	if(!new_ptr)
		return 0;

	if (*space != 0 || *page_no > 0x8FFFFFFF) {
		recv_sys->found_corrupt_log = TRUE;
		return(0);
	}

	*body = new_ptr;
	/*对mtr方法的调用*/
	new_ptr = recv_parse_or_apply_log_rec_body(*type, new_ptr, end_ptr, NULL, NULL);
	if(!new_ptr)
		return 0;

	return (new_ptr - ptr);
}

/*增加一个长度为len的数据对应新的LSN值*/
static dulint recv_calc_lsn_on_data_add(dulint lsn, ulint len)
{
	ulint frag_len;
	ulint lsn_len;

	frag_len = (ut_dulint_get_low(lsn) % OS_FILE_LOG_BLOCK_SIZE) - LOG_BLOCK_HDR_SIZE;
	ut_ad(frag_len < OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE);

	/*获得lsn的增量*/
	lsn_len = len + ((len + frag_len) / (OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE)) * (LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE);

	return ut_dulint_add(lsn, lsn_len);
}

void recv_check_incomplete_log_recs(byte* ptr, ulint len)
{
	ulint	i;
	byte	type;
	ulint	space;
	ulint	page_no;
	byte*	body;

	for (i = 0; i < len; i++) {
		ut_a(0 == recv_parse_log_rec(ptr, ptr + i, &type, &space, &page_no, &body));
	}
}

/*打印错误重做日志的诊断信息*/
static void recv_report_corrupt_log(byte* ptr, byte type, ulint space, ulint page_no)
{
	char* err_buf;

	fprintf(stderr,
		"InnoDB: ############### CORRUPT LOG RECORD FOUND\n"
		"InnoDB: Log record type %lu, space id %lu, page number %lu\n"
		"InnoDB: Log parsing proceeded successfully up to %lu %lu\n",
		(ulint)type, space, page_no,
		ut_dulint_get_high(recv_sys->recovered_lsn),
		ut_dulint_get_low(recv_sys->recovered_lsn));

	err_buf = ut_malloc(1000000);

	fprintf(stderr,
		"InnoDB: Previous log record type %lu, is multi %lu\n"
		"InnoDB: Recv offset %lu, prev %lu\n",
		recv_previous_parsed_rec_type,
		recv_previous_parsed_rec_is_multi,
		ptr - recv_sys->buf,
		recv_previous_parsed_rec_offset);

	if((ulint)(ptr - recv_sys->buf + 100) > recv_previous_parsed_rec_offset 
		&& (ulint)(ptr - recv_sys->buf + 100 - recv_previous_parsed_rec_offset) < 200000){
			ut_sprintf_buf(err_buf, recv_sys->buf + recv_previous_parsed_rec_offset - 100,
				ptr - recv_sys->buf + 200 - recv_previous_parsed_rec_offset);

			fprintf(stderr,
				"InnoDB: Hex dump of corrupt log starting 100 bytes before the start\n"
				"InnoDB: of the previous log rec,\n"
				"InnoDB: and ending 100 bytes after the start of the corrupt rec:\n%s\n",
				err_buf);
	}

	ut_free(err_buf);

	fprintf(stderr,
		"InnoDB: WARNING: the log file may have been corrupt and it\n"
		"InnoDB: is possible that the log scan did not proceed\n"
		"InnoDB: far enough in recovery! Please run CHECK TABLE\n"
		"InnoDB: on your InnoDB tables to check that they are ok!\n");
}

/*将recv_sys->buf缓冲区中的log解析成mtr recv_addr_t数据并存储hash table中*/
static ibool recv_parse_log_recs(ibool store_to_hash)
{
	byte*	ptr;
	byte*	end_ptr;
	ulint	single_rec;
	ulint	len;
	ulint	total_len;
	dulint	new_recovered_lsn;
	dulint	old_lsn;
	byte	type;
	ulint	space;
	ulint	page_no;
	byte*	body;
	ulint	n_recs;

	ut_ad(mutex_own(&(log_sys->mutex)));
	ut_ad(!ut_dulint_is_zero(recv_sys->parse_start_lsn));

loop:
	ptr = recv_sys->buf + recv_sys->recovered_offset;
	end_ptr = recv_sys->buf + recv_sys->len;

	if(ptr == end_ptr)
		return FALSE;

	single_rec = (ulint)*ptr & MLOG_SINGLE_REC_FLAG;
	if(single_rec || *ptr == MLOG_DUMMY_RECORD){
		old_lsn = recv_sys->recovered_lsn;

		len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);
		if(len == 0 || recv_sys->found_corrupt_log){ /*mtr 方法调用错误，进行日志诊断*/
			if (recv_sys->found_corrupt_log)
				recv_report_corrupt_log(ptr, type, space, page_no);

			return FALSE;
		}

		/*计算恢复的lsn*/
		new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);
		if(ut_dulint_cmp(new_recovered_lsn, recv_sys->scanned_lsn) > 0) /*恢复的lsn已经大于扫描过的lsn,这可能是个异常，退出函数*/
			return FALSE;

		recv_previous_parsed_rec_type = (ulint)type;
		recv_previous_parsed_rec_offset = recv_sys->recovered_offset;
		recv_previous_parsed_rec_is_multi = 0;

		/*更新恢复的偏移量和lsn*/
		recv_sys->recovered_offset += len;
		recv_sys->recovered_lsn = new_recovered_lsn;

		if(log_debug_writes)
			fprintf(stderr, "InnoDB: Parsed a single log rec type %lu len %lu space %lu page no %lu\n", (ulint)type, len, space, page_no);

		if(type == MLOG_DUMMY_RECORD){

		}
		else if(store_to_hash) /*将恢复指令存到hash表当中*/
			recv_add_to_hash_table(type, space, page_no, body, ptr + len, old_lsn, recv_sys->recovered_lsn);
		else{
#ifdef UNIV_LOG_DEBUG
			recv_check_incomplete_log_recs(ptr, len);
#endif
		}
	}
	else{
		total_len = 0;
		n_recs = 0;

		for(;;){
			/*将日志解析成记录*/
			len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);
			if(len == 0 || recv_sys->found_corrupt_log){
				if(recv_sys->found_corrupt_log)
					recv_report_corrupt_log(ptr, type, space, page_no);
				return FALSE;
			}

			recv_previous_parsed_rec_type = (ulint)type;
			recv_previous_parsed_rec_offset = recv_sys->recovered_offset + total_len;
			recv_previous_parsed_rec_is_multi = 1;

			if ((!store_to_hash) && (type != MLOG_MULTI_REC_END)){
#ifdef UNIV_LOG_DEBUG
				recv_check_incomplete_log_recs(ptr, len);
#endif	
			}

			if(log_debug_writes)
				fprintf(stderr, "InnoDB: Parsed a multi log rec type %lu len %lu space %lu page no %lu\n", (ulint)type, len, space, page_no);

			total_len += len;
			n_recs++;

			ptr += len;

			if (type == MLOG_MULTI_REC_END) /*已经到末尾了*/
				break;
		}
		/*修改recovered_lsn*/
		new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);
		if(ut_dulint_cmp(new_recovered_lsn, recv_sys->scanned_lsn) > 0) /*恢复的lsn已经大于扫描过的lsn,这可能是个异常，退出函数*/
			return FALSE;

		/*调整log的恢复位置*/
		ptr = recv_sys->buf + recv_sys->recovered_offset;
		/*批量将recv_addr加入到hash table当中*/
		for(;;){
			old_lsn = recv_sys->recovered_lsn;
			len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);
			/*调整lsn和offset*/
			recv_sys->recovered_offset += len;
			recv_sys->recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);
			if(type == MLOG_MULTI_REC_END)
				break;

			if(store_to_hash)
				recv_add_to_hash_table(type, space, page_no, body, ptr + len, old_lsn, new_recovered_lsn);
			else{

			}
			ptr += len;
		}
	}
	/*一直继续，直到解析完成为止*/
	goto loop;
}

static ibool recv_sys_add_to_parsing_buf(byte* log_block, dulint scanned_lsn)
{
	ulint	more_len;
	ulint	data_len;
	ulint	start_offset;
	ulint	end_offset;

	ut_ad(ut_dulint_cmp(scanned_lsn, recv_sys->scanned_lsn) >= 0);

	if(ut_dulint_is_zero(recv_sys->parse_start_len)) /*不能开始解析buf中的数据，可能是没有找到开始点*/
		return FALSE;

	if(ut_dulint_cmp(recv_sys->parse_start_lsn, scanned_lsn) >= 0) /*解析开始的lsn比scanned_ls还大*/
		return FALSE;
	else if(ut_dulint_cmp(recv_sys->scanned_lsn, scanned_lsn) >= 0)
		return FALSE;
	else if(ut_dulint_cmp(recv_sys->parse_start_lsn, scanned_lsn) > 0)
		more_len = ut_dulint_minus(scanned_lsn, recv_sys->parse_start_lsn);
	else
		more_len = ut_dulint_minus(scanned_lsn, recv_sys->scanned_lsn);

	if(more_len == 0)
		return FALSE;

	ut_ad(data_len > more_len);

	/*计算开始的偏移量*/
	start_offset = data_len - more_len;
	if(start_offset < LOG_BLOCK_HDR_SIZE)
		start_offset = LOG_BLOCK_HDR_SIZE;
	/*计算结束的offset*/
	end_offset = data_len;
	if(end_offset > OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE)
		end_offset = OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;

	ut_ad(start_offset <= end_offset);
	if(start_offset < end_offset){ /*将block中的数据拷贝至recv_sys->buf当汇总*/
		ut_memcpy(recv_sys->buf + recv_sys->len, log_block + start_offset, end_offset - start_offset);
		recv_sys->len += end_offset - start_offset;

		ut_a(recv_sys->len <= RECV_PARSING_BUF_SIZE);
	}

	return TRUE;
}

/*移除应recover的数据并将未recover的数据移动到recv_buf的最前面*/
static void recv_sys_justify_left_parsing_buf()
{
	ut_memmove(recv_sys->buf, recv_sys->buf + recv_sys->recovered_offset,
		recv_sys->len - recv_sys->recovered_offset);

	/*修正偏移*/
	recv_sys->len -= recv_sys->recovered_offset;
	recv_sys->recovered_offset = 0;
}

ibool recv_scan_log_recs(ibool apply_automatically, ulint available_memory, ibool store_to_hash, byte* buf, ulint len, 
	dulint start_lsn, dulint* contiguous_lsn, dulint* group_scanned_lsn)
{
	byte*	log_block;
	ulint	no;
	dulint	scanned_lsn;
	ibool	finished;
	ulint	data_len;
	ibool	more_data;

	ut_ad(ut_dulint_get_low(start_lsn) % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_ad(len % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_ad(len > 0);
	ut_a(apply_automatically <= TRUE);
	ut_a(store_to_hash <= TRUE);

	finished = FALSE;

	log_block = buf;
	scanned_lsn = start_lsn;
	more_data = FALSE;
	
	/*scan buf直到其末尾,对块的合法性判断，将合法性的块log写入到recv_sys->buf中*/
	while(log_block < buf + len && !finished){
		no = log_block_get_hdr_no(log_block); /*获得block的序号*/
		if(no != log_block_convert_lsn_to_no(scanned_lsn) || !log_block_checksum_is_ok_old_format(log_block)){ /*校验block的合法性*/
			if(no == log_block_convert_lsn_to_no(scanned_lsn) && !log_block_checksum_is_ok_old_format(log_block)){ /*块序号不相等且校验码不对*/
				fprintf(stderr,
					"InnoDB: Log block no %lu at lsn %lu %lu has\n"
					"InnoDB: ok header, but checksum field contains %lu, should be %lu\n",
					no, ut_dulint_get_high(scanned_lsn), ut_dulint_get_low(scanned_lsn),
					log_block_get_checksum(log_block), log_block_calc_checksum(log_block));
			}
			
			finished = TRUE;
			break;
		}

		/*检查是否flush到磁盘上*/
		if(log_block_get_flush_bit(log_block)){
			if(ut_dulint_cmp(scanned_lsn, *contiguous_lsn) > 0)
				*contiguous_lsn = scanned_lsn;
		}

		data_len = log_block_get_data_len(log_block);
		if ((store_to_hash || (data_len == OS_FILE_LOG_BLOCK_SIZE))
			&& (ut_dulint_cmp(ut_dulint_add(scanned_lsn, data_len), recv_sys->scanned_lsn) > 0) /*不属于scanned范围*/
			&& (recv_sys->scanned_checkpoint_no > 0) 
			&& (log_block_get_checkpoint_no(log_block) < recv_sys->scanned_checkpoint_no) /*小于扫描的checkpoint点*/
			&& (recv_sys->scanned_checkpoint_no - log_block_get_checkpoint_no(log_block)> 0x80000000)) {
				finished = TRUE;
				ut_error;
				break;
		}

		/*刚开始恢复，设置各种lsn*/
		if(ut_dulint_is_zero(recv_sys->parse_start_lsn) && (log_block_get_first_rec_group(log_block) > 0)){
			recv_sys->parse_start_lsn = ut_dulint_add(scanned_lsn, log_block_get_first_rec_group(log_block));

			recv_sys->scanned_lsn = recv_sys->parse_start_lsn;
			recv_sys->recovered_lsn = recv_sys->parse_start_lsn;
		}

		scanned_lsn = ut_dulint_add(scanned_lsn, data_len);
		if(ut_dulint_cmp(scanned_lsn, recv_sys->scanned_lsn) > 0){
			if (recv_sys->len + 4 * OS_FILE_LOG_BLOCK_SIZE >= RECV_PARSING_BUF_SIZE){
				fprintf(stderr, "InnoDB: Error: log parsing buffer overflow. Recovery may have failed!\n");

				recv_sys->found_corrupt_log = TRUE;
			}
			else if(!recv_sys->found_corrupt_log) /*将日志数据放入recv_sys->buff*/
				more_data = recv_sys_add_to_parsing_buf(log_block, scanned_lsn); /*将数据放入recv_addr当中*/
		
			recv_sys->scanned_lsn = scanned_lsn;
			recv_sys->scanned_lsn = log_block_get_checkpoint_no(log_block);
		}

		if(data_len < OS_FILE_LOG_BLOCK_SIZE)
			finished = TRUE;
		else /*完成一块的parse*/
			log_block += OS_FILE_LOG_BLOCK_SIZE;
	}

	*group_scanned_lsn = scanned_lsn;
	if(recv_needed_recovery || (recv_is_from_backup && !recv_is_making_a_backup)){
		recv_scan_print_counter ++;
		if(finished || recv_scan_print_counter % 80 == 0){
			fprintf(stderr, 
				"InnoDB: Doing recovery: scanned up to log sequence number %lu %lu\n",
				ut_dulint_get_high(*group_scanned_lsn),
				ut_dulint_get_low(*group_scanned_lsn));
		}
	}

	if(more_data && !recv_sys->found_corrupt_log){
		/*尝试解析log*/
		recv_parse_log_recs(store_to_hash);
		if(store_to_hash && mem_heap_get_size(recv_sys->heap) > available_memory && apply_automatically){ /*批量将recv_addr中的数据应用到页上*/
			recv_apply_hashed_log_recs(FALSE);
		}

		if(recv_sys->recovered_offset > RECV_PARSING_BUF_SIZE / 4) /*偏移大于解析buffer的1/4，进行buffer数据移动*/
			recv_sys_justify_left_parsing_buf();
	}

	return finished;
}

/*从group文件读取一段日志数据恢复到page当中*/
static void recv_group_scan_log_recs(log_group_t* group, dulint* contiguous_lsn, dulint* group_scanned_lsn)
{
	ibool finished;
	dulint start_len;
	dulint end_lsn;

	finished = FALSE;
	start_lsn = *contiguous_lsn;

	while(!finished){
		end_lsn = ut_dulint_add(start_lsn, RECV_SCAN_SIZE);
		/*从group file中读取一段日志数据到log_sys->buf当中*/
		log_group_read_log_seg(LOG_RECOVER, log_sys->buf, group, start_lsn, end_lsn);

		/*将读出的数据进行日志恢复到对应的page当中*/
		finished = recv_scan_log_recs(TRUE, buf_pool_get_curr_size() - RECV_POOL_N_FREE_BLOCKS * UNIV_PAGE_SIZE,
			TRUE, log_sys->buf, RECV_SCAN_SIZE, start_lsn, contiguous_lsn, group_scanned_lsn);

		start_lsn = end_lsn;
	}

	if(log_debug_writes){
		fprintf(stderr,
			"InnoDB: Scanned group %lu up to log sequence number %lu %lu\n",
			group->id, ut_dulint_get_high(*group_scanned_lsn), ut_dulint_get_low(*group_scanned_lsn));
	}
}

/*从checkpoint中开始恢复数据*/
ulint recv_recovery_from_checkpoint_start(ulint type, dulint limit_lsn, dulint min_flushed_lsn, dulint max_flushed_lsn)
{
	log_group_t*	group;
	log_group_t*	max_cp_group;
	log_group_t*	up_to_date_group;
	ulint		max_cp_field;
	dulint		checkpoint_lsn;
	dulint		checkpoint_no;
	dulint		old_scanned_lsn;
	dulint		group_scanned_lsn;
	dulint		contiguous_lsn;
	dulint		archived_lsn;
	ulint		capacity;
	byte*		buf;
	byte		log_hdr_buf[LOG_FILE_HDR_SIZE];
	ulint		err;

	/*从checkpoint开始恢复，limit_lsn必须有个上限*/
	ut_ad(type != LOG_CHECKPOINT || ut_dulint_cmp(limit_lsn, ut_dulint_max) == 0);
	if(type == LOG_CHECKPOINT){ /*建立recv_sys*/
		recv_sys_create();
		recv_sys_init(FALSE, buf_pool_get_curr_size());
	}

	if(sys_force_recovery >= SRV_FORCE_NO_LOG_REDO){
		fprintf(stderr, "InnoDB: The user has set SRV_FORCE_NO_LOG_REDO on\n");
		fprintf(stderr, "InnoDB: Skipping log redo\n");

		return DB_SUCCESS;
	}

	sync_order_checks_on = TRUE;
	recv_recovery_on = TRUE;

	recv_sys->limit_lsn = limit_lsn;
	mutex_enter(&(log_sys->mutex));

	/*查找最近的checkpoint信息在groups各个组当中*/
	err = recv_find_max_checkpoint(&max_cp_group, &max_cp_field);
	if(err != DB_SUCCESS){
		mutex_exit(&(log_sys->mutex));
		return err;
	}

	/*将checkpoint信息读取到group->checkbuf当中*/
	log_group_read_checkpoint_info(max_cp_group, max_cp_field);
	buf = log_sys->checkpoint_buf;
	checkpoint_lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
	checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);
	archived_lsn = mach_read_from_8(buf + LOG_CHECKPOINT_ARCHIVED_LSN);

	/*读取文件头信息*/
	fil_io(OS_FILE_READ | OS_FILE_LOG, TRUE, max_cp_group->space_id, 0, 0, LOG_FILE_HDR_SIZE, log_hdr_buf, max_cp_group);
	if (0 == ut_memcmp(log_hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "ibbackup", ut_strlen("ibbackup"))) {
		fprintf(stderr,
			"InnoDB: The log file was created by ibbackup --restore at\n"
			"InnoDB: %s\n", log_hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP);

		ut_memcpy(log_hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "    ", 4);
		/*取消备份标志，并且回写到文件当中，因为这个文件已经作为group的恢复主本*/
		fil_io(OS_FILE_WRITE | OS_FILE_LOG, TRUE, max_cp_group->space_id, 0, 0, OS_FILE_LOG_BLOCK_SIZE, log_hdr_buf, max_cp_group);
	}

	/*统一各个组归档的file_no和offset*/
	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	while(group){
		log_checkpoint_get_nth_group_info(buf, group->id, &(group->archived_file_no), &(group->archived_offset));
		group = UT_LIST_GET_NEXT();
	}

	if(type == LOG_CHECKPOINT){
		recv_sys->parse_start_lsn = checkpoint_lsn; /*从checkpoint出开始做日志恢复*/
		recv_sys->scanned_lsn = checkpoint_lsn;		
		recv_sys->scanned_checkpoint_no = 0;
		recv_sys->recovered_lsn = checkpoint_lsn;

		/*需要对日志进行重做，恢复page中的数据*/
		if(ut_dulint_cmp(checkpoint_lsn, max_flushed_lsn) != 0 
			|| ut_dulint_cmp(checkpoint_lsn, min_flushed_lsn) != 0){ /*设置开始恢复日志数据的操作*/
				recv_needed_recovery = TRUE;
				ut_print_timestamp(stderr);

				fprintf(stderr,
					"  InnoDB: Database was not shut down normally.\n"
					"InnoDB: Starting recovery from log files...\n");

				fprintf(stderr, 
					"InnoDB: Starting log scan based on checkpoint at\n"
					"InnoDB: log sequence number %lu %lu\n",
					ut_dulint_get_high(checkpoint_lsn),
					ut_dulint_get_low(checkpoint_lsn));
		}
	}

	contiguous_lsn = ut_dulint_align_down(recv_sys->scanned_lsn, OS_FILE_LOG_BLOCK_SIZE);
	/*从归档日志中恢复页*/
	if(type == LOG_ARCHIVE){
		group = recv_sys->archive_group;
		capacity = log_group_get_capacity(group);

		if ((ut_dulint_cmp(recv_sys->scanned_lsn, ut_dulint_add(checkpoint_lsn, capacity)) > 0)
			|| (ut_dulint_cmp(checkpoint_lsn, ut_dulint_add(recv_sys->scanned_lsn, capacity)) > 0)){
				mutex_exit(&(log_sys->mutex));
				return DB_SUCCESS;
		}

		/*从group文件读取一段日志数据恢复到page当中*/
		recv_group_scan_log_recs(group, &contiguous_lsn, &group_scanned_lsn);
		if(ut_dulint_cmp(recv_sys->scanned_lsn, checkpoint_lsn) < 0){
			mutex_exit(&(log_sys->mutex));
			return DB_ERROR;
		}

		group->scanned_lsn = group_scanned_lsn;
		up_to_date_group = group;
	}
	else
		up_to_date_group = max_cp_group;

	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	if((type == LOG_ARCHIVE) && (group == recv_sys->archive_group))
		group = UT_LIST_GET_NEXT(log_groups, group);

	/*各个group刷盘的数据的位置可能不一样，所以要逐group进行数据恢复*/
	while(group){
		old_scanned_lsn = recv_sys->scanned_lsn;
		recv_group_scan_log_recs(group, &contiguous_lsn, &group_scanned_lsn);

		group->scanned_lsn = group_scanned_lsn;
		if(ut_dulint_cmp(old_scanned_lsn, group_scanned_lsn) < 0)
			up_to_date_group = group;

		if ((type == LOG_ARCHIVE) && (group == recv_sys->archive_group))
				group = UT_LIST_GET_NEXT(log_groups, group);
	
		group = UT_LIST_GET_NEXT(log_groups, group);
	}

	/*已恢复的lsn小于checkpoint，表示已经无数据需要进行恢复*/
	if(ut_dulint_cmp(recv_sys->recovered_lsn, checkpoint_lsn) < 0){
		mutex_exit(&(log_sys->mutex));
		if(ut_dulint_cmp(recv_sys->recovered_lsn, limit_lsn) >= 0)
			return DB_SUCCESS;

		ut_error;
		return DB_ERROR;
	}

	/*刷新log_sys的的信息*/
	log_sys->next_checkpoint_lsn = checkpoint_lsn;
	log_sys->next_checkpoint_no = ut_dulint_add(checkpoint_no, 1);

	log_sys->archived_lsn = archived_lsn;
	/*将up_to_date_group信息复制到各个group当中,为了组之间的信息同步*/
	recv_synchronize_groups(up_to_date_group);

	/*更新lsn*/
	log_sys->lsn = recv_sys->recovered_lsn;
	/*更新最后一个block到log_sys->buf当中，因为last_block可能还有空间可以写入日志*/
	ut_memcpy(log_sys->buf, recv_sys->last_block, OS_FILE_LOG_BLOCK_SIZE);

	log_sys->buf_free = ut_dulint_get_low(log_sys->lsn) % OS_FILE_LOG_BLOCK_SIZE;
	log_sys->buf_next_to_write = log_sys->buf_free;
	log_sys->written_to_some_lsn = log_sys->lsn;
	log_sys->written_to_all_lsn = log_sys->lsn;
	log_sys->last_checkpoint_lsn = checkpoint_lsn;

	log_sys->next_checkpoint_no = ut_dulint_add(checkpoint_no, 1);

	if(ut_dulint_cmp(archived_lsn, ut_dulint_max) == 0) /*archived_lsn是个无限大的数，表示没有开启归档功能*/
		log_sys->archiving_state = LOG_ARCH_OFF;

	mutex_enter(SYS_MUTEX);
	recv_sys->apply_log_recs = TRUE;
	mutex_exit(SYS_MUTEX);

	mutex_exit(&(log_sys->mutex));

	sync_order_checks_on = FALSE;

	return DB_SUCCESS;
}

/*完成从checkpoint出开始日志恢复page的过程*/
void recv_recovery_from_checkpoint_finish()
{
	if(srv_force_recovery < SRV_FORCE_NO_TRX_UNDO)
		trx_rollback_or_clean_all_without_sess();

	/*将recv_addr的hash表中的数据恢复到page中*/
	if(srv_force_recovery < SRV_FORCE_NO_LOG_REDO)
		recv_apply_hashed_log_recs(TRUE);

	if(log_debug_writes)
		fprintf(stderr, "InnoDB: Log records applied to the database\n");

	/*进行了redo log恢复数据*/
	if(recv_needed_recovery){
		trx_sys_print_mysql_master_log_pos();
		trx_sys_print_mysql_binlog_offset();
	}

	if (recv_sys->found_corrupt_log) {
		fprintf(stderr,
			"InnoDB: WARNING: the log file may have been corrupt and it\n"
			"InnoDB: is possible that the log scan or parsing did not proceed\n"
			"InnoDB: far enough in recovery. Please run CHECK TABLE\n"
			"InnoDB: on your InnoDB tables to check that they are ok!\n"
			"InnoDB: It may be safest to recover your InnoDB database from\n"
			"InnoDB: a backup!\n");
	}
	/*redo日志恢复结束*/
	recv_recovery_on = FALSE;

#ifndef UNIV_LOG_DEBUG
	recv_sys_free();
#endif
}

/*截取recv_sys->last_block中的信息做日志重新写入*/
void recv_reset_logs(dulint lsn, ulint arch_log_no, ibool new_logs_created)
{
	log_group_t* group;
	ut_ad(mutex_own(&(log_sys->mutex)));

	log_sys->lsn = ut_dulint_align_up(lsn, OS_FILE_LOG_BLOCK_SIZE);
	group = UT_LIST_GET_FIRST(log_sys->log_groups);
	while(group){
		group->lsn = log_sys->lsn;
		group->lsn_offset = LOG_FILE_HDR_SIZE;

		group->archived_file_no = arch_log_no;		
		group->archived_offset = 0;

		if (!new_logs_created)
			recv_truncate_group(group, group->lsn, group->lsn, group->lsn, group->lsn);

		group = UT_LIST_GET_NEXT(log_groups, group);
	}

	log_sys->buf_next_to_write = 0;
	log_sys->written_to_some_lsn = log_sys->lsn;
	log_sys->written_to_all_lsn = log_sys->lsn;

	log_sys->next_checkpoint_no = ut_dulint_zero;
	log_sys->last_checkpoint_lsn = ut_dulint_zero;

	log_sys->archived_lsn = log_sys->lsn;

	log_block_init(log_sys->buf, log_sys->lsn);
	log_block_set_first_rec_group(log_sys->buf, LOG_BLOCK_HDR_SIZE);

	log_sys->buf_free = LOG_BLOCK_HDR_SIZE;
	log_sys->lsn = ut_dulint_add(log_sys->lsn, LOG_BLOCK_HDR_SIZE);

	mutex_exit(&(log_sys->mutex));

	/* Reset the checkpoint fields in logs */
	log_make_checkpoint_at(ut_dulint_max, TRUE);
	log_make_checkpoint_at(ut_dulint_max, TRUE)
}

void recv_reset_log_files_for_backup(char* log_dir, ulint n_log_files, ulint log_file_size, dulint lsn)
{
	os_file_t	log_file;
	ibool		success;
	byte*		buf;
	ulint		i;
	char		name[5000];
	
	buf = ut_malloc(LOG_FILE_HDR_SIZE + OS_FILE_LOG_BLOCK_SIZE);
	
	/*建立了个多个文件，并设置了长度*/
	for (i = 0; i < n_log_files; i++) {
		sprintf(name, "%sib_logfile%lu", log_dir, i);

		log_file = os_file_create_simple(name, OS_FILE_CREATE, OS_FILE_READ_WRITE, &success);
		if (!success) {
			printf("InnoDB: Cannot create %s. Check that the file does not exist yet.\n", name);
			exit(1);
		}

		printf("Setting log file size to %lu %lu\n", ut_get_high32(log_file_size), log_file_size & 0xFFFFFFFF);
		success = os_file_set_size(name, log_file, log_file_size & 0xFFFFFFFF, ut_get_high32(log_file_size));

		if (!success) {
			printf("InnoDB: Cannot set %s size to %lu %lu\n", name, ut_get_high32(log_file_size), log_file_size & 0xFFFFFFFF);
			exit(1);
		}

		os_file_flush(log_file);
		os_file_close(log_file);
	}

	/* We pretend there is a checkpoint at lsn + LOG_BLOCK_HDR_SIZE */
	log_reset_first_header_and_checkpoint(buf, lsn);
	
	log_block_init_in_old_format(buf + LOG_FILE_HDR_SIZE, lsn);
	log_block_set_first_rec_group(buf + LOG_FILE_HDR_SIZE, LOG_BLOCK_HDR_SIZE);
	sprintf(name, "%sib_logfile%lu", log_dir, 0);

	log_file = os_file_create_simple(name, OS_FILE_OPEN, OS_FILE_READ_WRITE, &success);
	if (!success) {
		printf("InnoDB: Cannot open %s.\n", name);
		exit(1);
	}

	os_file_write(name, log_file, buf, 0, 0, LOG_FILE_HDR_SIZE + OS_FILE_LOG_BLOCK_SIZE);
	os_file_flush(log_file);
	os_file_close(log_file);

	ut_free(buf);
}

/*从group的archive file中读取redo log来进行数据恢复*/
static ibool log_group_recover_from_archive_file(log_group_t* group)
{
	os_file_t file_handle;
	dulint	start_lsn;
	dulint	file_end_lsn;
	dulint	dummy_lsn;
	dulint	scanned_lsn;
	ulint	len;
	ibool	ret;
	byte*	buf;
	ulint	read_offset;
	ulint	file_size;
	ulint	file_size_high;
	int	input_char;
	char	name[10000];

try_open_again:	
	buf = log_sys->buf;
	/* Add the file to the archive file space; open the file */
	log_archived_file_name_gen(name, group->id, group->archived_file_no);
	fil_reserve_right_to_open();

	file_handle = os_file_create(name, OS_FILE_OPEN, OS_FILE_LOG, OS_FILE_AIO, &ret);

	if (ret == FALSE) {
		fil_release_right_to_open();
ask_again:
		fprintf(stderr, "InnoDB: Do you want to copy additional archived log files\n InnoDB: to the directory\n");
		fprintf(stderr, "InnoDB: or were these all the files needed in recovery?\n");
		fprintf(stderr, "InnoDB: (Y == copy more files; N == this is all)?");

		input_char = getchar();
		if (input_char == (int) 'N')
			return(TRUE);
		else if (input_char == (int) 'Y')
			goto try_open_again;
		else
			goto ask_again;
	}

	ret = os_file_get_size(file_handle, &file_size, &file_size_high);
	ut_a(ret);
	ut_a(file_size_high == 0);
	fprintf(stderr, "InnoDB: Opened archived log file %s\n", name);
			
	ret = os_file_close(file_handle);
	/*用于恢复数据的archive file一定是大于文件头长度的*/
	if (file_size < LOG_FILE_HDR_SIZE) {
		fprintf(stderr, "InnoDB: Archive file header incomplete %s\n", name);
		return(TRUE);
	}

	ut_a(ret);
	fil_release_right_to_open();
	
	/* Add the archive file as a node to the space */
	fil_node_create(name, 1 + file_size / UNIV_PAGE_SIZE, group->archive_space_id);
	ut_a(RECV_SCAN_SIZE >= LOG_FILE_HDR_SIZE);

	/* Read the archive file header */
	fil_io(OS_FILE_READ | OS_FILE_LOG, TRUE, group->archive_space_id, 0, 0, LOG_FILE_HDR_SIZE, buf, NULL);

	/* Check if the archive file header is consistent */
	if (mach_read_from_4(buf + LOG_GROUP_ID) != group->id || mach_read_from_4(buf + LOG_FILE_NO) != group->archived_file_no) {
		fprintf(stderr,"InnoDB: Archive file header inconsistent %s\n", name);
		return(TRUE);
	}

	if (!mach_read_from_4(buf + LOG_FILE_ARCH_COMPLETED)) {
		fprintf(stderr, "InnoDB: Archive file not completely written %s\n", name);
		return(TRUE);
	}
	
	start_lsn = mach_read_from_8(buf + LOG_FILE_START_LSN);
	file_end_lsn = mach_read_from_8(buf + LOG_FILE_END_LSN);

	if (ut_dulint_is_zero(recv_sys->scanned_lsn)) {
		if (ut_dulint_cmp(recv_sys->parse_start_lsn, start_lsn) < 0) {
			fprintf(stderr, "InnoDB: Archive log file %s starts from too big a lsn\n", name);	    
			return(TRUE);
		}
	
		recv_sys->scanned_lsn = start_lsn;
	}
	
	if (ut_dulint_cmp(recv_sys->scanned_lsn, start_lsn) != 0) {
		fprintf(stderr, "InnoDB: Archive log file %s starts from a wrong lsn\n", name);
		return(TRUE);
	}

	read_offset = LOG_FILE_HDR_SIZE;
	
	for (;;) {
		len = RECV_SCAN_SIZE;

		if (read_offset + len > file_size)
			len = ut_calc_align_down(file_size - read_offset, OS_FILE_LOG_BLOCK_SIZE);


		if (len == 0)
			break;
	
		if (log_debug_writes) {
			fprintf(stderr, "InnoDB: Archive read starting at lsn %lu %lu, len %lu from file %s\n",
					ut_dulint_get_high(start_lsn), ut_dulint_get_low(start_lsn), len, name);
		}
		/*从archive file中读取一段日志数据*/
		fil_io(OS_FILE_READ | OS_FILE_LOG, TRUE, group->archive_space_id, read_offset / UNIV_PAGE_SIZE,
			read_offset % UNIV_PAGE_SIZE, len, buf, NULL);
		/*将读出的数据进行日志恢复到对应的page当中*/
		ret = recv_scan_log_recs(TRUE, buf_pool_get_curr_size() - RECV_POOL_N_FREE_BLOCKS * UNIV_PAGE_SIZE,
				TRUE, buf, len, start_lsn, &dummy_lsn, &scanned_lsn);

		if (ut_dulint_cmp(scanned_lsn, file_end_lsn) == 0)
			return(FALSE);

		if (ret) {
			fprintf(stderr, "InnoDB: Archive log file %s does not scan right\n", name);	    
			return(TRUE);
		}
		
		read_offset += len;
		start_lsn = ut_dulint_add(start_lsn, len);

		ut_ad(ut_dulint_cmp(start_lsn, scanned_lsn) == 0);
	}

	return(FALSE);
}

ulint
recv_recovery_from_archive_start(dulint	min_flushed_lsn, dulint	limit_lsn, ulint first_log_no)	
{
	log_group_t*	group;
	ulint		group_id;
	ulint		trunc_len;
	ibool		ret;
	ulint		err;
	
	/*建立一个全局的recv_sys对象*/
	recv_sys_create();
	recv_sys_init(FALSE, buf_pool_get_curr_size());

	sync_order_checks_on = TRUE;
	
	recv_recovery_on = TRUE;
	recv_recovery_from_backup_on = TRUE;

	recv_sys->limit_lsn = limit_lsn;

	group_id = 0;
	group = UT_LIST_GET_FIRST(log_sys->log_groups);

	while (group) {
		if (group->id == group_id)
 			break;
		
		group = UT_LIST_GET_NEXT(log_groups, group);
	}

	if (!group){
		fprintf(stderr, "InnoDB: There is no log group defined with id %lu!\n", group_id);
		return(DB_ERROR);
	}

	group->archived_file_no = first_log_no;

	recv_sys->parse_start_lsn = min_flushed_lsn;

	recv_sys->scanned_lsn = ut_dulint_zero;
	recv_sys->scanned_checkpoint_no = 0;
	recv_sys->recovered_lsn = recv_sys->parse_start_lsn;

	recv_sys->archive_group = group;

	ret = FALSE;
	
	mutex_enter(&(log_sys->mutex));

	while (!ret) {
		ret = log_group_recover_from_archive_file(group);

		/* Close and truncate a possible processed archive file from the file space */	
		trunc_len = UNIV_PAGE_SIZE * fil_space_get_size(group->archive_space_id);
		if (trunc_len > 0) 
			fil_space_truncate_start(group->archive_space_id, trunc_len);

		group->archived_file_no++;
	}

	if (ut_dulint_cmp(recv_sys->recovered_lsn, limit_lsn) < 0) {
		if (ut_dulint_is_zero(recv_sys->scanned_lsn))
			recv_sys->scanned_lsn = recv_sys->parse_start_lsn;

		mutex_exit(&(log_sys->mutex));

		err = recv_recovery_from_checkpoint_start(LOG_ARCHIVE, limit_lsn, ut_dulint_max, ut_dulint_max);
		if (err != DB_SUCCESS)
			return(err);

		mutex_enter(&(log_sys->mutex));
	}

	if (ut_dulint_cmp(limit_lsn, ut_dulint_max) != 0){
		recv_apply_hashed_log_recs(FALSE);
		recv_reset_logs(recv_sys->recovered_lsn, 0, FALSE);
	}

	mutex_exit(&(log_sys->mutex));
	sync_order_checks_on = FALSE;

	return(DB_SUCCESS);
}

void recv_recovery_from_archive_finish()
{
	recv_recovery_from_checkpoint_finish();
	recv_recovery_from_backup_on = FALSE;
}









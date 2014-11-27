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
				(nth_page_in_file << UNIV_PAGE_SIZE_SHIFT)
				& 0xFFFFFFFF,
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






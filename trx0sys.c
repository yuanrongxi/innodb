#include "trx0sys.h"

#include "fsp0fsp.h"
#include "mtr0mtr.h"
#include "trx0trx.h"
#include "trx0rseg.h"
#include "trx0undo.h"
#include "srv0srv.h"
#include "trx0purge.h"
#include "log0log.h"
#include "os0file.h"


trx_sys_t*			trx_sys = NULL;
trx_doublewrite_t*	trx_doublewrite = NULL;

char				trx_sys_mysql_master_log_name[TRX_SYS_MYSQL_LOG_NAME_LEN];
ib_longlong			trx_sys_mysql_master_log_pos = -1;

/*判断page_no对应的页数据是否在doublewrite中*/
ibool trx_doublewrite_page_inside(ulint page_no)
{
	if(trx_doublewrite == NULL)
		return FALSE;

	/*在doublewirte 的第一块上*/
	if(page_no >= trx_doublewrite->block1 && page_no < trx_doublewrite->block1 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE)
		return TRUE;
	/*在第二块上*/
	if(page_no >= trx_doublewrite->block2 && page_no < trx_doublewrite->block2 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE)
		return TRUE;

	return FALSE;
}

/*数据库启动时，创建并初始化doublewrite*/
static void trx_doublewrite_init(byte* doublewrite)
{
	trx_doublewrite = mem_alloc(sizeof(trx_doublewrite_t));

	os_do_not_call_flush_at_each_write = TRUE;

	mutex_create(&(trx_doublewrite->mutex));
	mutex_set_level(&(trx_doublewrite->mutex), SYNC_DOUBLEWRITE);

	trx_doublewrite->first_free = 0;
	/*读取两个块的第一个page_no*/
	trx_doublewrite->block1 = mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_BLOCK1);
	trx_doublewrite->block2 = mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_BLOCK2);
	/*在程序的堆上分配doublewrite的空间，不是在buffer pool上申请*/
	trx_doublewrite->write_buf_unaligned =  ut_malloc((1 + 2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) * UNIV_PAGE_SIZE);
	/*地址对齐，便以访问*/
	trx_doublewrite->write_buf = ut_align(trx_doublewrite->write_buf_unaligned, UNIV_PAGE_SIZE);

	trx_doublewrite->buf_block_arr = mem_alloc(2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * sizeof(void*));
}

void trx_sys_create_doublewrite_buf()
{
	page_t*	page;
	page_t*	page2;
	page_t*	new_page;
	byte*	doublewrite;
	byte*	fseg_header;
	ulint	page_no;
	ulint	prev_page_no;
	ulint	i;
	mtr_t	mtr;

	/*已经初始化了*/
	if(trx_doublewrite != NULL)
		return;

start_again:
	mtr_start(&mtr);

	/*读取trx_sys的header page*/
	page = buf_page_get(TRX_SYS_SPACE, TRX_SYS_PAGE_NO, RW_X_LATCH, &mtr);
	buf_page_dbg_add_level(page, SYNC_NO_ORDER_CHECK);

	/*定位到存有doublewrite信息*/
	doublewrite = page + TRX_SYS_DOUBLEWRITE;
	if(mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_MAGIC) == TRX_SYS_DOUBLEWRITE_MAGIC_N){
		/*读取page中的doublewirte信息，并初始化trx_doublewrite*/
		trx_doublewrite_init(doublewrite);
		mtr_commit(&mtr);
	}
	else{ /*磁盘上没有doublewrite信息，创建一个新的*/
		fprintf(stderr, "InnoDB: Doublewrite buffer not found: creating new\n");

		if (buf_pool_get_curr_size() < (2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE + FSP_EXTENT_SIZE / 2 + 100) * UNIV_PAGE_SIZE) {
			fprintf(stderr, "InnoDB: Cannot create doublewrite buffer: you must\n"
				"InnoDB: increase your buffer pool size.\n"
				"InnoDB: Cannot continue operation.\n");

			exit(1);
		}
		/*创建一个表空间段，*/
		page2 = fseg_create(TRX_SYS_SPACE, TRX_SYS_PAGE_NO, TRX_SYS_DOUBLEWRITE + TRX_SYS_DOUBLEWRITE_FSEG, &mtr);
		buf_page_dbg_add_level(page2, SYNC_NO_ORDER_CHECK);

		if(page2 == NULL){
			fprintf(stderr,
				"InnoDB: Cannot create doublewrite buffer: you must\n"
				"InnoDB: increase your tablespace size.\n"
				"InnoDB: Cannot continue operation.\n");

			exit(-1);
		}

		fseg_header = page + TRX_SYS_DOUBLEWRITE + TRX_SYS_DOUBLEWRITE_FSEG;
		prev_page_no = 0;

		/*为doublewrite分配连续的页*/
		for (i = 0; i < 2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE + FSP_EXTENT_SIZE / 2; i++) {
			page_no = fseg_alloc_free_page(fseg_header, prev_page_no + 1, FSP_UP, &mtr);
			if (page_no == FIL_NULL) {
				fprintf(stderr,
					"InnoDB: Cannot create doublewrite buffer: you must\n"
					"InnoDB: increase your tablespace size.\n"
					"InnoDB: Cannot continue operation.\n");

				exit(1);
			}

			new_page = buf_page_get(TRX_SYS_SPACE, page_no, RW_X_LATCH, &mtr);
			buf_page_dbg_add_level(new_page, SYNC_NO_ORDER_CHECK);
			/*设置doublewrite魔法校验字*/
			mlog_write_ulint(new_page + FIL_PAGE_DATA, TRX_SYS_DOUBLEWRITE_MAGIC_N, MLOG_4BYTES, &mtr);
			/*写入doublewrite的起始页信息*/
			if (i == FSP_EXTENT_SIZE / 2) {
				mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_BLOCK1, page_no, MLOG_4BYTES, &mtr);
				mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_REPEAT + TRX_SYS_DOUBLEWRITE_BLOCK1, page_no, MLOG_4BYTES, &mtr);
			} 
			else if (i == FSP_EXTENT_SIZE / 2 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
					mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_BLOCK2, page_no, MLOG_4BYTES, &mtr);
					mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_REPEAT + TRX_SYS_DOUBLEWRITE_BLOCK2, page_no, MLOG_4BYTES, &mtr);
			} 
			else if (i > FSP_EXTENT_SIZE / 2)
				ut_a(page_no == prev_page_no + 1);

			prev_page_no = page_no;
		}
		/*写入doublewrite头页的魔法字*/
		mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_MAGIC, TRX_SYS_DOUBLEWRITE_MAGIC_N, MLOG_4BYTES, &mtr);
		mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_MAGIC + TRX_SYS_DOUBLEWRITE_REPEAT, TRX_SYS_DOUBLEWRITE_MAGIC_N, MLOG_4BYTES, &mtr);
		mtr_commit(&mtr);
		/*立即建立checkpoint,防止异常崩溃重演doublewrite的建立过程。这将会是个死循环，因为doublewrite本来就是为redo log提供可靠保证的*/
		log_make_checkpoint_at(ut_dulint_max, TRUE);

		fprintf(stderr, "InnoDB: Doublewrite buffer created\n");

		goto start_again;
	}
}

/*通过doublewrite存有的页信息恢复表空间对应的不完整页数据*/
void trx_sys_doublewrite_restore_corrupt_pages()
{
	byte*	buf;
	byte*	read_buf;
	byte*	unaligned_read_buf;
	ulint	block1;
	ulint	block2;
	byte*	page;
	byte*	doublewrite;
	ulint	space_id;
	ulint	page_no;
	ulint	i;

	/*从磁盘表空间中读取sys_trx header page*/
	unaligned_read_buf = ut_malloc(2 * UNIV_PAGE_SIZE);
	read_buf = ut_align(unaligned_read_buf, UNIV_PAGE_SIZE);

	fil_io(OS_FILE_READ, TRUE, TRX_SYS_SPACE, TRX_SYS_PAGE_NO, 0, UNIV_PAGE_SIZE, read_buf, NULL);

	doublewrite = read_buf + TRX_SYS_DOUBLEWRITE;
	/*校验魔法字，并读取doublewrite状态*/
	if (mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_MAGIC) == TRX_SYS_DOUBLEWRITE_MAGIC_N) {
		trx_doublewrite_init(doublewrite);
		block1 = trx_doublewrite->block1;
		block2 = trx_doublewrite->block2;
	}
	else
		goto leave_func;
	/*读取doublewrite block1 block2的数据*/
	fil_io(OS_FILE_READ, TRUE, TRX_SYS_SPACE, block1, 0,
		TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE, buf, NULL);
	fil_io(OS_FILE_READ, TRUE, TRX_SYS_SPACE, block2, 0,
		TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
		buf + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE, NULL);

	page = buf;
	/*检查所有doublewrite中存储的page对应的表空间数据是否完整，如果不完整，将doublewrite页数据同步写入到表空间中*/
	for(i = 0; i < TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * 2; i++){
		space_id = mach_read_from_4(page + FIL_PAGE_SPACE);
		page_no = mach_read_from_4(page + FIL_PAGE_OFFSET);

		if (!fil_check_adress_in_tablespace(space_id, page_no)) { /*(space_id, page_no)不属于任何的表空间，这可能磁盘数据发生了错误*/
			fprintf(stderr,
				"InnoDB: Warning: an inconsistent page in the doublewrite buffer\n"
				"InnoDB: space id %lu page number %lu, %lu'th page in dblwr buf.\n",
				space_id, page_no, i);
		}
		else if(space_id == TRX_SYS_SPACE && ((page_no >= block1 && page_no < block1 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE)
			|| (page_no >= block2 && page_no < block2 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE))){ /*是doublewrite自己的page,不需要做任何事*/
		}
		else{
			fil_io(OS_FILE_READ, TRUE, space_id, page_no, 0, UNIV_PAGE_SIZE, read_buf, NULL);
			if(buf_page_is_corrupted(read_buf)){ /*表空间的对应页数据不完整*/
				fprintf(stderr,
					"InnoDB: Warning: database page corruption or a failed\n"
					"InnoDB: file read of page %lu.\n", page_no);
				fprintf(stderr, "InnoDB: Trying to recover it from the doublewrite buffer.\n");
				/*doublewrite对应的页数据也不完整，整个page无法恢复*/
				if (buf_page_is_corrupted(page)){
					fprintf(stderr, "InnoDB: Also the page in the doublewrite buffer is corrupt.\n"
						"InnoDB: Cannot continue operation.\n");
					exit(1);
				}
				/*将doublewrite中的page写入到对应表空间中，替换原来不完整的页*/
				fil_io(OS_FILE_WRITE, TRUE, space_id, page_no, 0, UNIV_PAGE_SIZE, page, NULL);

				fprintf(stderr, "InnoDB: Recovered the page from the doublewrite buffer.\n");
			}
		}

		page += UNIV_PAGE_SIZE;
	}

	fil_flush_file_spaces(FIL_TABLESPACE);

leave_func:
	ut_free(unaligned_read_buf);
}

/*判断in_trx是否在trx_sys->trx_list中*/
ibool trx_in_trx_list(trx_t* in_trx)
{
	trx_t* trx;

	ut_ad(mutex_own(&(kernel_mutex)));

	trx = UT_LIST_GET_FIRST(trx_sys->trx_list);
	while(trx != NULL){
		if(trx == in_trx)
			return TRUE;
		trx = UT_LIST_GET_NEXT(trx_list, trx);
	}

	return FALSE;
}

/*flush max_trx_id*/
void trx_sys_flush_max_trx_id()
{
	trx_sysf_t*	sys_header;
	mtr_t		mtr;

	mtr_start(&mtr);

	sys_header = trx_sysf_get(&mtr);
	/*将trx_sys->max_trx_id写入到sys_header头页中，并产生一个mini transcation提交*/
	mlog_write_dulint(sys_header + TRX_SYS_TRX_ID_STORE, trx_sys->max_trx_id, MLOG_8BYTES, &mtr);

	mtr_commit(&mtr);
}

/*对上层mysql binlog的保存*/
void trx_sys_update_mysql_binlog_offset(char* file_name, ib_longlong offset, ulint field, mtr_t* mtr)
{
	trx_sysf_t* sys_header;

	if(ut_strlen(file_name) >= TRX_SYS_MYSQL_LOG_NAME_LEN)
		return ;

	sys_header = trx_sysf_get(mtr);
	/*写入mysql binlog在trx_sys头页的魔法校验字*/
	if(mach_read_from_4(sys_header + field + TRX_SYS_MYSQL_LOG_MAGIC_N_FLD) != TRX_SYS_MYSQL_LOG_MAGIC_N){
		mlog_write_ulint(sys_header + field + TRX_SYS_MYSQL_LOG_MAGIC_N_FLD, TRX_SYS_MYSQL_LOG_MAGIC_N, MLOG_4BYTES, mtr);
	}

	if(0 != ut_memcmp(sys_header + field + TRX_SYS_MYSQL_LOG_NAME, file_name, 1 + ut_strlen(file_name))){
		mlog_write_string(sys_header + field + TRX_SYS_MYSQL_LOG_NAME, file_name, 1 + ut_strlen(file_name), mtr);
	}

	if (mach_read_from_4(sys_header + field + TRX_SYS_MYSQL_LOG_OFFSET_HIGH) > 0 || (offset >> 32) > 0) {
			mlog_write_ulint(sys_header + field + TRX_SYS_MYSQL_LOG_OFFSET_HIGH, (ulint)(offset >> 32), MLOG_4BYTES, mtr);
	}

	mlog_write_ulint(sys_header + field + TRX_SYS_MYSQL_LOG_OFFSET_LOW, (ulint)(offset & 0xFFFFFFFF), MLOG_4BYTES, mtr);	
}

/*对sys_trx保存的binlog信息进行打印*/
void trx_sys_print_mysql_binlog_offset_from_page(byte* page)	
{
	trx_sysf_t*	sys_header;

	sys_header = page + TRX_SYS;
	
	if (mach_read_from_4(sys_header + TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_MAGIC_N_FLD) == TRX_SYS_MYSQL_LOG_MAGIC_N){
		printf("ibbackup: Last MySQL binlog file position %lu %lu, file name %s\n",
		mach_read_from_4(sys_header + TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_OFFSET_HIGH),
		mach_read_from_4(sys_header + TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_OFFSET_LOW),
		sys_header + TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_NAME);
	}
}

void trx_sys_print_mysql_binlog_offset(void)
{
	trx_sysf_t*	sys_header;
	mtr_t		mtr;

	mtr_start(&mtr);

	sys_header = trx_sysf_get(&mtr);

	if (mach_read_from_4(sys_header + TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_MAGIC_N_FLD) != TRX_SYS_MYSQL_LOG_MAGIC_N){
			mtr_commit(&mtr);
			return;
	}

	fprintf(stderr, "InnoDB: Last MySQL binlog file position %lu %lu, file name %s\n",
		mach_read_from_4(sys_header + TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_OFFSET_HIGH),
		mach_read_from_4(sys_header + TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_OFFSET_LOW),
		sys_header + TRX_SYS_MYSQL_LOG_INFO + TRX_SYS_MYSQL_LOG_NAME);

	mtr_commit(&mtr);
}

/*打印mysql binlog状态信息，并初始化全局变量trx_sys_mysql_master_log_name、trx_sys_mysql_master_log_pos*/
void trx_sys_print_mysql_master_log_pos(void)
{
	trx_sysf_t*	sys_header;
	mtr_t		mtr;
	
	mtr_start(&mtr);

	sys_header = trx_sysf_get(&mtr);

	if (mach_read_from_4(sys_header + TRX_SYS_MYSQL_MASTER_LOG_INFO + TRX_SYS_MYSQL_LOG_MAGIC_N_FLD) != TRX_SYS_MYSQL_LOG_MAGIC_N) {
		mtr_commit(&mtr);
		return;
	}

	fprintf(stderr, "InnoDB: In a MySQL replication slave the last master binlog file\n"
		"InnoDB: position %lu %lu, file name %s\n",
		mach_read_from_4(sys_header + TRX_SYS_MYSQL_MASTER_LOG_INFO + TRX_SYS_MYSQL_LOG_OFFSET_HIGH),
		mach_read_from_4(sys_header + TRX_SYS_MYSQL_MASTER_LOG_INFO + TRX_SYS_MYSQL_LOG_OFFSET_LOW),
		sys_header + TRX_SYS_MYSQL_MASTER_LOG_INFO + TRX_SYS_MYSQL_LOG_NAME);

	/* Copy the master log position info to global variables we can
	use in ha_innobase.cc to initialize glob_mi to right values */

	ut_memcpy(trx_sys_mysql_master_log_name, sys_header + TRX_SYS_MYSQL_MASTER_LOG_INFO + TRX_SYS_MYSQL_LOG_NAME, TRX_SYS_MYSQL_LOG_NAME_LEN);

	trx_sys_mysql_master_log_pos = (((ib_longlong)mach_read_from_4(sys_header + TRX_SYS_MYSQL_MASTER_LOG_INFO + TRX_SYS_MYSQL_LOG_OFFSET_HIGH)) << 32) + (ib_longlong)
		mach_read_from_4(sys_header + TRX_SYS_MYSQL_MASTER_LOG_INFO + TRX_SYS_MYSQL_LOG_OFFSET_LOW);

	mtr_commit(&mtr);
}

/*在trx_sys找到一个空闲的rollback segment的slot槽位*/
ulint trx_sysf_rseg_find_free(mtr_t* mtr)
{
	trx_sysf_t*	sys_header;
	ulint		page_no;
	ulint		i;

	ut_ad(mutex_own(&kernel_mutex));

	sys_header = trx_sysf_get(mtr);
	for(i = 0; i < TRX_SYS_N_RSEGS; i++){
		page_no = trx_sysf_rseg_get_page_no(sys_header, i, mtr);
		if(page_no == FIL_NULL)
			return i;
	}

	return ULINT_UNDEFINED;
}

/*创建一个trx_sys header page并初始化*/
static void trx_sysf_create(mtr_t* mtr)
{
	trx_sysf_t*	sys_header;
	ulint		slot_no;
	page_t*		page;
	ulint		page_no;
	ulint		i;

	ut_ad(mtr);

	mtr_x_lock(fil_space_get_latch(TRX_SYS_SPACE), mtr);
	mutex_enter(&kernel_mutex);

	page = fseg_create(TRX_SYS_SPACE, 0, TRX_SYS + TRX_SYS_FSEG_HEADER, mtr);
	ut_a(buf_frame_get_page_no(page) == TRX_SYS_PAGE_NO);

	buf_page_dbg_add_level(page, SYNC_TRX_SYS_HEADER);

	sys_header = trx_sysf_get(mtr);
	mlog_write_dulint(sys_header + TRX_SYS_TRX_ID_STORE, ut_dulint_create(0, 1), MLOG_8BYTES, mtr);

	for (i = 0; i < TRX_SYS_N_RSEGS; i++) 
		trx_sysf_rseg_set_page_no(sys_header, i, FIL_NULL, mtr);

	page_no = trx_rseg_header_create(TRX_SYS_SPACE, ULINT_MAX, &slot_no, mtr);

	ut_a(slot_no == TRX_SYS_SYSTEM_RSEG_ID);
	ut_a(page_no != FIL_NULL);

	mutex_exit(&kernel_mutex);
}

/*数据库启动时对trx_sys的初始化*/
void trx_sys_init_at_db_start()
{
	trx_sysf_t*	sys_header;
	mtr_t*		mtr;

	mtr_start(&mtr);

	ut_ad(trx_sys == NULL);
	mutex_enter(&kernel_mutex);

	trx_sys = mem_alloc(sizeof(trx_sys_t));
	sys_header = trx_sysf_get(&mtr);
	/*初始化rseg object list*/
	trx_rseg_list_and_array_init(sys_header, &mtr);

	trx_sys->latest_rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);

	trx_sys->max_trx_id = ut_dulint_add(ut_dulint_align_up(mtr_read_dulint(sys_header + TRX_SYS_TRX_ID_STORE, MLOG_8BYTES, &mtr), TRX_SYS_TRX_ID_WRITE_MARGIN), 
		2 * TRX_SYS_TRX_ID_WRITE_MARGIN);

	/*对trx事务对象进行初始化*/
	UT_LIST_INIT(trx_sys->mysql_trx_list);				
	trx_lists_init_at_db_start();

	if (UT_LIST_GET_LEN(trx_sys->trx_list) > 0) {
		fprintf(stderr, "InnoDB: %lu transaction(s) which must be rolled back or cleaned up\n",
			UT_LIST_GET_LEN(trx_sys->trx_list));

		fprintf(stderr, "InnoDB: Trx id counter is %lu %lu\n", 
			ut_dulint_get_high(trx_sys->max_trx_id),
			ut_dulint_get_low(trx_sys->max_trx_id));
	}

	UT_LIST_INIT(trx_sys->view_list);

	trx_purge_sys_create();

	mutex_exit(&kernel_mutex);

	mtr_commit(&mtr);
}

void trx_sys_create()
{
	mtr_t	mtr;

	mtr_start(&mtr);

	trx_sysf_create(&mtr);

	mtr_commit(&mtr);

	trx_sys_init_at_db_start();
}



#ifndef __log0recv_h
#define __log0recv_h

#include "univ.h"
#include "ut0byte.h"
#include "page0types.h"
#include "hash0hash.h"
#include "log0log.h"

/*********************************************api************************************/
/*获得checkpoint信息*/
ibool				recv_read_cp_info_for_backup(byte* hdr, dulint* lsn, ulint* offset, ulint fsp_limit, dulint* cp_no, dulint* cp_no, dulint* first_header_lsn);
/*扫描一段log片段，返回有效block的n_byte_scanned长度和scanned_checkpoint_no*/
void				recv_scan_log_seg_for_backup(byte* buf, ulint buf_len, dulint* scanned_lsn, ulint* scanned_checkpoint_no, ulint n_byte_scanned);

UNIV_INLINE ibool	recv_recovery_is_on();

UNIV_INLINE ibool	recv_recovery_from_backup_is_on();
/*将space,page_no对应的recv_addr中的日志数据写入到page页当中*/
void				recv_recover_page(ibool recover_backup, ibool just_read_in, page_t* page, ulint space, ulint page_no);
/*开始进行checkpoint处进行redo log数据恢复*/
ulint				recv_recovery_from_checkpoint_start(ulint type, dulint limit_lsn, dulint min_flushed_lsn, dulint max_flushed_lsn);
/*结束checkpoint的redo log数据恢复操作*/
void				recv_recovery_from_checkpoint_finish();

ibool				recv_scan_log_recs(ibool apply_automatically, ulint available_memory, ibool store_to_hash, byte* buf, 
							ulint len, dulint start_lsn, dulint* contiguous_lsn, dulint* group_scanned_lsn);

void				recv_reset_logs(dulint lsn, ulint arch_log_no, ibool new_logs_created);

void				recv_reset_log_file_for_backup(char* log_dir, ulint n_log_files, ulint log_file_size, dulint lsn);

void				recv_sys_create();

void				recv_sys_init(ibool recover_from_backup, ulint available_memory);
/*将recv_sys->addr_hash中的recv_data_t的日志全部应用到对应的page中*/
void				recv_apply_hashed_log_recs(ibool allow_ibuf);

void				recv_apply_log_recs_for_backup(ulint n_data_files, char** data_files, ulint* file_sizes);

ulint				recv_recovery_from_archive_start(ulint type, dulint min_flushed_lsn, dulint limit_lsn, ulint first_log_no);

void				recv_recovery_from_archive_finish();

void				recv_compare_spaces();

void				recv_compare_spaces_low(ulint space1, ulint space2, ulint n_pages);

/********************************************************/
typedef struct recv_data_struct	recv_data_t;
struct recv_data_struct
{
	recv_data_t*	next;	/*下一个recv_data_t,next的地址后面接了一大块内存，用于存储rec body*/
};

typedef struct recv_struct recv_t;
struct recv_struct
{
	byte			type;			/*log类型*/
	ulint			len;			/*当前记录数据长度*/
	recv_data_t*	data;			/*当前的记录数据list*/
	dulint			start_lsn;		/*mtr起始lsn*/	
	dulint			end_lsn;		/*mtr结尾lns*/
	UT_LIST_NODE_T(recv_t)	rec_list;
};

typedef struct recv_addr_struct recv_addr_t;
struct recv_addr_struct
{
	ulint			state;		/*状态，RECV_NOT_PROCESSED、RECV_BEING_PROCESSED、RECV_PROCESSED*/	
	ulint			space;		/*space的ID*/
	ulint			page_no;	/*页序号*/
	UT_LIST_BASE_NODE_T(recv_t) rec_list;
	hash_node_t		addr_hash;
};

typedef struct recv_sys_struct recv_sys_t;
struct recv_sys_struct
{
	mutex_t			mutex;				/*保护锁*/
	ibool			apply_log_recs;		/*正在应用log record到page中*/
	ibool			apply_batch_on;		/*批量应用log record标志*/
	
	dulint			lsn;						
	ulint			last_log_buf_size;

	byte*			last_block;				/*恢复时最后的块内存缓冲区*/
	byte*			last_block_buf_start;	/*最后块内存缓冲区的起始位置，因为last_block是512地址对齐的，需要这个变量记录free的地址位置*/
	byte*			buf;					/*从日志块中读取的重做日志信息数据*/
	ulint			len;					/*buf有效的日志数据长度*/

	dulint			parse_start_lsn;		/*开始parse的lsn*/
	dulint			scanned_lsn;			/*已经扫描过的lsn序号*/	

	ulint			scanned_checkpoint_no;	/*恢复日志的checkpoint 序号*/
	ulint			recovered_offset;		/*恢复位置的偏移量*/

	dulint			recovered_lsn;			/*恢复的lsn位置*/
	dulint			limit_lsn;				/*日志恢复最大的lsn,暂时在日志重做的过程没有使用*/

	ibool			found_corrupt_log;		/*是否开启日志恢复诊断*/

	log_group_t*	archive_group;		

	mem_heap_t*		heap;				/*recv sys的内存分配堆*/
	hash_table_t*	addr_hash;			/*recv_addr的hash表，以space id和page no为KEY*/
	ulint			n_addrs;			/*addr_hash中包含recv_addr的个数*/
};

extern recv_sys_t*		recv_sys;
extern ibool			recv_recovery_on;
extern ibool			recv_no_ibuf_operations;
extern ibool			recv_needed_recovery;

extern ibool			recv_is_making_a_backup;

/*2M*/
#define RECV_PARSING_BUF_SIZE	(2 * 1024 * 1204)

#define RECV_SCAN_SIZE			(4 * UNIV_PAGE_SIZE)

/*recv_addr_t->state type*/
#define RECV_NOT_PROCESSED		71
#define RECV_BEING_READ			72
#define RECV_BEING_PROCESSED	73
#define RECV_PROCESSED			74

#define RECV_REPLICA_SPACE_ADD	1

#define RECV_POOL_N_FREE_BLOCKS	(ut_min(256, buf_pool_get_curr_size() / 8))

#include "log0recv.inl"

#endif







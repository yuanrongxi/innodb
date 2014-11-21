#ifndef LOG0LOG_H_
#define LOG0LOG_H_

#include "univ.h"
#include "ut0byte.h"
#include "sync0rw.h"
#include "sync0sync.h"

/*log_flush_up_to的等待类型*/
#define LOG_NO_WAIT			91
#define LOG_WAIT_ONE_GROUP	92
#define	LOG_WAIT_ALL_GROUPS	93
/*log group的最大数量*/
#define LOG_MAX_N_GROUPS	32

/*archive的状态*/
#define LOG_ARCH_ON		71
#define LOG_ARCH_STOPPING	72
#define LOG_ARCH_STOPPING2	73
#define LOG_ARCH_STOPPED	74
#define LOG_ARCH_OFF		75

typedef struct log_group_struct
{
	ulint			id;					/*log group id*/
	ulint			n_files;			/*group包含的日志文件个数*/
	ulint			file_size;			/*日志文件大小，包括文件头*/
	ulint			space_id;			/*group对应的space id*/
	ulint			state;				/*log group状态，LOG_GROUP_OK、LOG_GROUP_CORRUPTED*/
	dulint			lsn;				/*log group的lsn坐标*/
	dulint			lsn_offset;			/*lsn的偏移量*/
	ulint			n_pending_writes;	/*本group 刷盘所填充的字节数*/

	byte**			file_header_bufs;	/*文件头缓冲区*/
	
	byte**			archive_file_header_bufs;
	ulint			archive_space_id;	/**/
	ulint			archived_file_no;
	ulint			archived_offset;
	ulint			next_archived_file_no;
	ulint			next_archived_offset;

	dulint			scanned_lsn;
	byte*			checkpoint_buf;

	UT_LIST_NODE_T(log_group_t) log_groups;
}log_group_t;

typedef struct log_struct
{
	byte			pad;
	dulint			lsn;				/*log的序列号,实际上是一个日志文件偏移量*/
	
	ulint			buf_free;
	
	mutex_t			mutex;				/*log保护的mutex*/
	byte*			buf;				/*log缓冲区*/
	ulint			buf_size;			/*log缓冲区长度*/
	ulint			max_buf_free;		/*在log buffer刷盘后，推荐buf_free的最大值，超过这个值会被强制刷盘*/
	
	ulint			old_buf_free;		/*上次写时buf_free的值*/
	dulint			old_lsn;			/*上次写时的lsn*/

	ibool			check_flush_or_checkpoint; /*需要日志写盘或者是需要刷新一个log checkpoint的标识*/

	UT_LIST_BASE_NODE_T(log_group_t) log_groups;

	ulint			buf_next_to_write;	/*下一次开始写入磁盘的buf偏移位置*/
	dulint			written_to_some_lsn;/**/
	dulint			written_to_all_lsn;

	dulint			flush_lsn;			/*flush的lsn*/
	ulint			flush_end_offset;
	ulint			n_pending_writes;	/*正在调用fil_flush的个数*/

	os_event_t		no_flush_event;		/*处于flush过程中的信号等待*/

	ibool			one_flushed;		/*一个log group被刷盘后这个值会设置成TRUE*/
	os_event_t		one_flushed_event;

	ulint			n_log_ios;
	ulint			n_log_ios_old;
	time_t			last_printout_time;

	ulint			max_modified_age_async;
	ulint			max_modified_age_sync;
	ulint			adm_checkpoint_interval;
	ulint			max_checkpoint_age_async;
	ulint			max_checkpoint_age;
	dulint			next_checkpoint_no;
	dulint			last_checkpoint_lsn;
	dulint			next_checkpoint_lsn;
	ulint			n_pending_checkpoint_writes;
	rw_lock_t		checkpoint_lock;	/*checkpoint的rw_lock_t,在checkpoint的时候，是独占这个latch*/
	byte*			checkpoint_buf;

	ulint			archiving_state;
	dulint			archived_lsn;
	dulint			max_archived_lsn_age_async;
	dulint			max_archived_lsn_age;
	dulint			next_archived_lsn;
	ulint			archiving_phase;
	ulint			n_pending_archive_ios;
	rw_lock_t		archive_lock;
	ulint			archive_buf_size;
	byte*			archive_buf;
	os_event_t		archiving_on;

	ibool			online_backup_state;	/*是否在backup*/
	dulint			online_backup_lsn;		/*backup时的lsn*/
}log_t;

extern	ibool	log_do_write;
extern 	ibool	log_debug_writes;

extern log_t*	log_sys;

/* Values used as flags */
#define LOG_FLUSH			7652559
#define LOG_CHECKPOINT		78656949
#define LOG_ARCHIVE			11122331
#define LOG_RECOVER			98887331

/*构建一个lsn序号*/
#define LOG_START_LSN				ut_dulint_create(0, 16 * OS_FILE_LOG_BLOCK_SIZE)
/*log的大小*/
#define LOG_BUFFER_SIZE				(srv_log_buffer_size * UNIV_PAGE_SIZE)		
/*需要存档的大小阈值*/
#define LOG_ARCHIVE_BUF_SIZE		(srv_log_buffer_size * UNIV_PAGE_SIZE / 4)

/*log 块头信息的偏移量*/
#define LOG_BLOCK_HDR_NO			0

#define LOG_BLOCK_FLUSH_BIT_MASK	0x80000000
/*log block head 的长度*/
#define LOG_BLOCK_HDR_DATA_LEN		4
	
#define LOG_BLOCK_FIRST_REC_GROUP	6

#define LOG_BLOCK_CHECKPOINT_NO		8

#define LOG_BLOCK_HDR_SIZE			12

#define LOG_BLOCK_CHECKSUM			4 /*log的checksum*/

#define LOG_BLOCK_TRL_SIZE			4

/* Offsets for a checkpoint field */
#define LOG_CHECKPOINT_NO			0
#define LOG_CHECKPOINT_LSN			8
#define LOG_CHECKPOINT_OFFSET		16
#define LOG_CHECKPOINT_LOG_BUF_SIZE	20
#define	LOG_CHECKPOINT_ARCHIVED_LSN	24
#define	LOG_CHECKPOINT_GROUP_ARRAY	32

#define LOG_CHECKPOINT_ARCHIVED_FILE_NO	0
#define LOG_CHECKPOINT_ARCHIVED_OFFSET	4

#define	LOG_CHECKPOINT_ARRAY_END	(LOG_CHECKPOINT_GROUP_ARRAY + LOG_MAX_N_GROUPS * 8) /*9x32*/
#define LOG_CHECKPOINT_CHECKSUM_1 	LOG_CHECKPOINT_ARRAY_END
#define LOG_CHECKPOINT_CHECKSUM_2 	(4 + LOG_CHECKPOINT_ARRAY_END)
#define LOG_CHECKPOINT_FSP_FREE_LIMIT	(8 + LOG_CHECKPOINT_ARRAY_END)
/*魔法字位置*/
#define LOG_CHECKPOINT_FSP_MAGIC_N	(12 + LOG_CHECKPOINT_ARRAY_END)
#define LOG_CHECKPOINT_SIZE			(16 + LOG_CHECKPOINT_ARRAY_END)

/*checkpoint magic value*/
#define LOG_CHECKPOINT_FSP_MAGIC_N_VAL	1441231243

/*log file header的偏移量，应该是在磁盘上的相对位置*/
#define LOG_GROUP_ID				0
#define LOG_FILE_START_LSN			4
#define LOG_FILE_NO					12
#define LOG_FILE_WAS_CREATED_BY_HOT_BACKUP 16
#define	LOG_FILE_ARCH_COMPLETED		OS_FILE_LOG_BLOCK_SIZE
#define LOG_FILE_END_LSN			(OS_FILE_LOG_BLOCK_SIZE + 4)
#define LOG_CHECKPOINT_1			OS_FILE_LOG_BLOCK_SIZE
#define LOG_CHECKPOINT_2			(3 * OS_FILE_LOG_BLOCK_SIZE)
#define LOG_FILE_HDR_SIZE			(4 * OS_FILE_LOG_BLOCK_SIZE)

#define LOG_GROUP_OK				301
#define LOG_GROUP_CORRUPTED			302
#endif

/********************************函数*********************************/
/*设置fsp_current_free_limit,这个改变有可能会产生一个checkpoint*/
void log_fsp_current_free_limit_set_and_checkpoint(ulint limit);

/*将str写入到log_sys当中，必须和buf_free凑成512的块，否则返回失败*/
UNIV_INLINE dulint	log_reserve_and_write_fast(byte*	str, ulint	len, dulint* start_lsn, ibool* success);
UNIV_INLINE void	log_release();
UNIV_INLINE VOID	log_free_check();
UNIV_INLINE dulint	log_get_lsn();
UNIV_INLINE dulint	log_get_online_backup_lsn_low();

dulint		log_reserve_and_open(ulint len);
void		log_write_low(byte* str, ulint str_len);
dulint		log_close();

ulint		log_group_get_capacity(log_group_t* group);
/*获得lsn在group中对应的文件和位置偏移*/
ulint		log_calc_where_lsn_is(int64_t* log_file_offset, dulint first_header_lsn, dulint lsn, ulint n_log_files, int64_t log_file_size);
void		log_group_set_fields(log_group_t* group, dulint lsn);

/*初始化log_sys*/
void		log_init();
/*初始化goup*/
void		log_group_init(ulint id, ulint n_files, ulint file_size, ulint space_id, ulint archive_space_id);
/*完成一个io操作*/
void		log_io_complete(log_group_t* group);

/*将日志文件flush到磁盘上,例如调用fsync,触发条件srv_flush_log_at_trx_commit = FALSE*/
void		log_flush_to_disk();

/*将buf 刷盘入group log file当中*/
void		log_group_write_buf(ulint type, log_group_t* group, byte* buf, ulint len, dulint start_lsn, ulint new_data_offset);

/*将sys_log中所有的group进行flush*/
void		log_flush_up_to(dulint lsn, ulint wait);

/********************************************************************
Advances the smallest lsn for which there are unflushed dirty blocks in the
buffer pool. NOTE: this function may only be called if the calling thread owns
no synchronization objects! */
ibool		log_preflush_pool_modified_pages(dulint new_oldest, ibool sync);


#include "log0log.inl"




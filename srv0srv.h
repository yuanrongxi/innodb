#ifndef __srv0srv_h__
#define __srv0srv_h__

#include "univ.h"
#include "sync0sync.h"
#include "os0sync.h"
#include "com0com.h"
#include "que0types.h"
#include "trx0types.h"

/*错误消息的缓冲区*/
extern char srv_fatal_errbuf[];

/* When this event is set the lock timeout and InnoDB monitor thread starts running */
extern os_event_t	srv_lock_timeout_thread_event;

/*数据文件自动扩展模式下，一次扩大的空间大小，以PAGE数量计算，8MB*/
#define SRV_AUTO_EXTEND_INCREMENT	(8 * ((1024 * 1024) / UNIV_PAGE_SIZE))

#define SRV_NEW_RAW    1
#define SRV_OLD_RAW    2

/*server参数，从初始化文件中读取*/
extern char*	srv_data_home;
extern char*	srv_arch_dir;

extern ulint	srv_n_data_files;
extern char**	srv_data_file_names;
extern ulint*	srv_data_file_sizes;
extern ulint*   srv_data_file_is_raw_partition;

extern ibool	srv_auto_extend_last_data_file;
extern ulint	srv_last_file_size_max;

extern ibool	srv_created_new_raw;

extern char**	srv_log_group_home_dirs;

extern ulint	srv_n_log_groups;
extern ulint	srv_n_log_files;
extern ulint	srv_log_file_size;
extern ibool	srv_log_archive_on;
extern ulint	srv_log_buffer_size;
extern ulint	srv_flush_log_at_trx_commit;

extern byte		srv_latin1_ordering[256];

extern ibool	srv_use_native_aio;		

extern ulint	srv_pool_size;
extern ulint	srv_mem_pool_size;
extern ulint	srv_lock_table_size;

extern ulint	srv_n_file_io_threads;

extern ibool	srv_archive_recovery;
extern dulint	srv_archive_recovery_limit_lsn;

extern ulint	srv_lock_wait_timeout;

extern char*    srv_unix_file_flush_method_str;
extern ulint    srv_unix_file_flush_method;
extern ulint	srv_force_recovery;
extern ulint	srv_thread_concurrency;

extern lint	srv_conc_n_threads;

extern ibool	srv_fast_shutdown;

extern ibool	srv_use_doublewrite_buf;

extern ibool    srv_set_thread_priorities;
extern int      srv_query_thread_priority;

/*-------------------------------------------*/
extern ulint	srv_n_rows_inserted;
extern ulint	srv_n_rows_updated;
extern ulint	srv_n_rows_deleted;
extern ulint	srv_n_rows_read;

extern ibool	srv_print_innodb_monitor;
extern ibool    srv_print_innodb_lock_monitor;
extern ibool    srv_print_innodb_tablespace_monitor;
extern ibool    srv_print_innodb_table_monitor;

extern ibool	srv_lock_timeout_and_monitor_active;
extern ibool	srv_error_monitor_active; 

extern ulint	srv_n_spin_wait_rounds;
extern ulint	srv_spin_wait_delay;
extern ibool	srv_priority_boost;

extern	ulint	srv_pool_size;
extern	ulint	srv_mem_pool_size;
extern	ulint	srv_lock_table_size;

extern	ulint	srv_sim_disk_wait_pct;
extern	ulint	srv_sim_disk_wait_len;
extern	ibool	srv_sim_disk_wait_by_yield;
extern	ibool	srv_sim_disk_wait_by_wait;

extern	ibool	srv_measure_contention;
extern	ibool	srv_measure_by_spin;

extern	ibool	srv_print_thread_releases;
extern	ibool	srv_print_lock_waits;
extern	ibool	srv_print_buf_io;
extern	ibool	srv_print_log_io;
extern	ibool	srv_print_parsed_sql;
extern	ibool	srv_print_latch_waits;

extern	ibool	srv_test_nocache;
extern	ibool	srv_test_cache_evict;

extern	ibool	srv_test_extra_mutexes;
extern	ibool	srv_test_sync;
extern	ulint	srv_test_n_threads;
extern	ulint	srv_test_n_loops;
extern	ulint	srv_test_n_free_rnds;
extern	ulint	srv_test_n_reserved_rnds;
extern	ulint	srv_test_n_mutexes;
extern	ulint	srv_test_array_size;

extern ulint	srv_activity_count;

extern mutex_t*	kernel_mutex_temp;
#define kernel_mutex (*kernel_mutex_temp)

#define SRV_MAX_N_IO_THREADS	100

extern char* srv_io_thread_op_info[];

/* Thread slot in the thread table */
typedef struct srv_slot_struct	srv_slot_t;
typedef srv_slot_t srv_table_t;

typedef struct srv_sys_struct{
	os_event_t				operational;
	com_endpoint_t*			endpoint;
	srv_table_t*			threads;
	UT_LIST_BASE_NODE_T(que_thr_t) tasks;	/*que thread任务管理队列*/
}srv_sys_t;

extern srv_sys_t*	srv_sys;

extern ulint	srv_n_threads_active[];

/* Alternatives for the field flush option in Unix; see the InnoDB manual about what these mean */
#define SRV_UNIX_FDATASYNC   1
#define SRV_UNIX_O_DSYNC     2
#define SRV_UNIX_LITTLESYNC  3
#define SRV_UNIX_NOSYNC      4

#define SRV_FORCE_IGNORE_CORRUPT	1
#define SRV_FORCE_NO_BACKGROUND		2
#define SRV_FORCE_NO_TRX_UNDO		3
#define SRV_FORCE_NO_IBUF_MERGE		4
#define SRV_FORCE_NO_UNDO_LOG_SCAN	5
#define SRV_FORCE_NO_LOG_REDO		6

#define	SRV_COM			1	/* threads serving communication and queries */
#define	SRV_CONSOLE		2	/* thread serving console */
#define	SRV_WORKER		3	/* threads serving parallelized queries and queries released from lock wait */
#define SRV_BUFFER		4	/* thread flushing dirty buffer blocks, not currently in use */
#define SRV_RECOVERY	5	/* threads finishing a recovery, not currently in use */
#define SRV_INSERT		6	/* thread flushing the insert buffer to disk,not currently in use */
#define SRV_MASTER		7   /* the master thread, (whose type number must be biggest) */

/*******************************************函数***********************************/
ulint					srv_boot();

void					srv_init();

void					srv_general_init();

ulint					srv_get_n_threads();

ulint					srv_get_thread_type();

ulint					srv_release_threads(ulint type, ulint n);

void*					srv_master_thread(void* arg);

ulint					srv_read_init_val(FILE* initfile, char* keyword, char* str_buf, ulint* num_val, ibool print_not_err);

void					srv_active_wake_master_thread();

void					srv_wake_master_thread();

void					srv_conc_enter_innodb(trx_t* trx);

void					srv_conc_force_enter_innodb(trx_t* trx);

void					srv_conc_force_exit_innodb(trx_t* trx);

void					srv_conc_exit_innodb(trx_t* trx);

ibool					srv_suspend_mysql_thread(que_thr_t* thr);

void					srv_release_mysql_thread_if_suspended(que_thr_t* thr);

void*					srv_lock_timeout_and_monitor_thread(void* arg);

void*					srv_error_monitor_thread(void* arg);

void					srv_sprintf_innodb_monitor(char* buf, ulint len);

#endif





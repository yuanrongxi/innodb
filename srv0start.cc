#include "os0proc.h"
#include "sync0sync.h"
#include "ut0mem.h"
#include "mem0mem.h"
#include "mem0pool.h"
#include "data0data.h"
#include "data0type.h"
#include "dict0dict.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "os0file.h"
#include "os0thread.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "rem0rec.h"
#include "rem0cmp.h"
#include "mtr0mtr.h"
#include "log0log.h"
#include "log0recv.h"
#include "page0page.h"
#include "page0cur.h"
#include "trx0trx.h"
#include "dict0boot.h"
#include "trx0sys.h"
#include "dict0crea.h"
#include "btr0btr.h"
#include "btr0pcur.h"
#include "btr0cur.h"
#include "btr0sea.h"
#include "rem0rec.h"
#include "srv0srv.h"
#include "que0que.h"
#include "com0com.h"
#include "usr0sess.h"
#include "lock0lock.h"
#include "trx0roll.h"
#include "trx0purge.h"
#include "row0ins.h"
#include "row0sel.h"
#include "row0upd.h"
#include "row0row.h"
#include "row0mysql.h"
#include "lock0lock.h"
#include "ibuf0ibuf.h"
#include "pars0pars.h"
#include "btr0sea.h"
#include "srv0start.h"
#include "que0que.h"

/*每个线程的AIO个数*/
#define SRV_N_PENDING_IOS_PER_THREAD		OS_AIO_N_PENDING_IOS_PER_THREAD
#define SRV_MAX_N_PENDING_SYNC_IOS			100

/*innodb同时能打开的文件数量*/
#define SRV_MAX_N_OPEN_FILES				500

#define SRV_LOG_SPACE_FIRST_ID				1000000000

ibool           srv_startup_is_before_trx_rollback_phase = FALSE;
ibool           srv_is_being_started		= FALSE;
ibool           srv_was_started				= FALSE;

ulint			srv_shutdown_state			= 0;

ibool			measure_cont				= FALSE;

os_file_t		files[1000];

mutex_t			ios_mutex;
ulint			ios;

ulint			n[SRV_MAX_N_IO_THREADS + 5];
os_thread_id_t	thread_ids[SRV_MAX_N_IO_THREADS + 5];

/* We use this mutex to test the return value of pthread_mutex_trylock
   on successful locking. HP-UX does NOT return 0, though Linux et al do. */
os_fast_mutex_t srv_os_test_mutex;

ibool			srv_os_test_mutex_is_locked = FALSE;

/*************************************************************************************************/
/*从.cnf文件的配置信息中读取每个数据文件的path和其空间大小*/
ibool srv_parse_data_file_paths_and_sizes(char* str, char*** data_file_names, ulint** data_file_sizes, ulint** data_file_is_raw_partition,
						ulint* n_data_files, ibool* is_auto_extending, ulint* max_auto_extend_size)
{
	char*	input_str;
	char*	endp;
	char*	path;
	ulint	size;
	ulint	i	= 0;

	*is_auto_extending = FALSE;
	*max_auto_extend_size = 0;

	input_str = str;

	/*首先校验字符串的格式化是否正确，例如：path:[M | G];path[M | G]....*/
	while (*str != '\0') {
		path = str;

		while ((*str != ':' && *str != '\0') || (*str == ':'&& (*(str + 1) == '\\' || *(str + 1) == '/'))) {
			str++;
		}

		if (*str == '\0')
			return(FALSE);

		str++;
		size = strtoul(str, &endp, 10);
		str = endp;

		if (*str != 'M' && *str != 'G')
			size = size / (1024 * 1024);
		else if (*str == 'G'){
			size = size * 1024;
			str++;
		} 
		else 
			str++;

		if (strlen(str) >= ut_strlen(":autoextend") && 0 == ut_memcmp(str, ":autoextend", ut_strlen(":autoextend"))) {
			str += ut_strlen(":autoextend");

			if (strlen(str) >= ut_strlen(":max:") && 0 == ut_memcmp(str, ":max:", ut_strlen(":max:"))) {
				str += ut_strlen(":max:");
				size = strtoul(str, &endp, 10);

				str = endp;
				if (*str != 'M' && *str != 'G')
					size = size / (1024 * 1024);
				else if (*str == 'G') {
					size = size * 1024;
					str++;
				} 
				else
					str++;
			}

			if (*str != '\0')
				return(FALSE);
		}

		if (strlen(str) >= 6 && *str == 'n' && *(str + 1) == 'e'  && *(str + 2) == 'w')
			str += 3;

		if (strlen(str) >= 3 && *str == 'r' && *(str + 1) == 'a' && *(str + 2) == 'w')
			str += 3;

		if (size == 0)
			return(FALSE);

		i++;

		if (*str == ';')
			str++;
		else if (*str != '\0')
			return(FALSE);
	}

	/*对字符串数据的读取*/
	*data_file_names = (char**)ut_malloc(i * sizeof(void*));
	*data_file_sizes = (ulint*)ut_malloc(i * sizeof(ulint));
	*data_file_is_raw_partition = (ulint*)ut_malloc(i * sizeof(ulint));

	*n_data_files = i;
	str = input_str;
	i = 0;

	while (*str != '\0') {
		path = str;

		/* Note that we must ignore the ':' in a Windows path */
		while ((*str != ':' && *str != '\0')
			|| (*str == ':' && (*(str + 1) == '\\' || *(str + 1) == '/'))) {
				str++;
		}

		if (*str == ':') {
			/* Make path a null-terminated string */
			*str = '\0';
			str++;
		}

		size = strtoul(str, &endp, 10);

		str = endp;

		if ((*str != 'M') && (*str != 'G')) {
			size = size / (1024 * 1024);
		} else if (*str == 'G') {
			size = size * 1024;
			str++;
		} else 
			str++;

		(*data_file_names)[i] = path;
		(*data_file_sizes)[i] = size;

		if (strlen(str) >= ut_strlen(":autoextend")&& 0 == ut_memcmp(str, ":autoextend",ut_strlen(":autoextend"))) {
			*is_auto_extending = TRUE;

			str += ut_strlen(":autoextend");
			if (strlen(str) >= ut_strlen(":max:") && 0 == ut_memcmp(str, ":max:",ut_strlen(":max:"))) {
				str += ut_strlen(":max:");

				size = strtoul(str, &endp, 10);

				str = endp;

				if (*str != 'M' && *str != 'G') {
					size = size / (1024 * 1024);
				} else if (*str == 'G') {
					size = size * 1024;
					str++;
				} else
					str++;

				*max_auto_extend_size = size;
			}

			if (*str != '\0') 
				return(FALSE);
		}

		(*data_file_is_raw_partition)[i] = 0;
		if (strlen(str) >= 6 && *str == 'n' && *(str + 1) == 'e' && *(str + 2) == 'w') {
			str += 3;
			(*data_file_is_raw_partition)[i] = SRV_NEW_RAW;
		}

		if (strlen(str) >= 3 && *str == 'r' && *(str + 1) == 'a' && *(str + 2) == 'w') {
			str += 3;

			if ((*data_file_is_raw_partition)[i] == 0) 
				(*data_file_is_raw_partition)[i] = SRV_OLD_RAW;
		}

		i++;

		if (*str == ';')
			str++;
	}

	return TRUE;
}

/*从.cnf文件中读取的字符串得到redo log group的目录*/
ibool srv_parse_log_group_home_dirs(char* str, char*** log_group_home_dirs)
{
	char*	input_str;
	char*	path;
	ulint	i	= 0;

	input_str = str;

	/*检查字符串的格式化有效性，path;path;...*/
	while(*str != '\0'){
		path = str;
		while (*str != ';' && *str != '\0')
			str++;

		i++;

		if (*str == ';')
			str++;
		else if (*str != '\0')
			return(FALSE);
	}

	/*都目录路径的读取*/
	*log_group_home_dirs = (char**) ut_malloc(i * sizeof(void*));

	str = input_str;
	i = 0;
	while (*str != '\0') {
		path = str;
		while (*str != ';' && *str != '\0')
			str++;

		if (*str == ';') {
			*str = '\0';
			str++;
		}

		(*log_group_home_dirs)[i] = path;
		i++;
	}
}

/*I/O thread的主体函数*/
static void* io_handler_thread(void* arg)
{
	ulint i;
	ulint segment;

	segment = *((ulint*)arg);

	for(i = 0;; i++){
		fil_aio_wait(segment);

		mutex_enter(&ios_mutex);
		ios++;
		mutex_exit(&ios_mutex);
	}

	return NULL;
}

#define SRV_PATH_SEPARATOR		"/"

/*路径分隔符的转换*/
void srv_normalize_path_for_win(char* str)
{
#ifdef __WIN__
	ulint	i;

	for (i = 0; i < ut_strlen(str); i++) {
		if (str[i] == '/')
			str[i] = '\\';
	}
#endif
}
/*为path最后一个字符加上'/',如果最后一个已经是'/',不必管。返回的字符串是malloc出来的，后面要free*/
char* srv_add_path_separator_if_needed(char* str)
{
	char* out_str;

	if (ut_strlen(str) == 0) 
		return(str);

	if (str[ut_strlen(str) - 1] == SRV_PATH_SEPARATOR[0]) {
		out_str = ut_malloc(ut_strlen(str) + 1);
		sprintf(out_str, "%s", str);
		return(out_str);
	}
		
	out_str = ut_malloc(ut_strlen(str) + 2);
	sprintf(out_str, "%s%s", str, SRV_PATH_SEPARATOR);

	return(out_str);
}

/*计算一个文件的大小，传入的参数是这个文件的page数量, 获得低位32的整形数*/
static ulint srv_calc_low32(ulint file_size)
{
	return(0xFFFFFFFF & (file_size << UNIV_PAGE_SIZE_SHIFT));
}

/*计算一个文件的大小，传入的参数是这个文件的page数量, 获得高位32的整形数*/
static ulint srv_calc_high32(ulint file_size)
{
	return(file_size >> (32 - UNIV_PAGE_SIZE_SHIFT));
}

/*打开个一个redo log file*/
static ulint open_or_create_log_file(ibool create_new_db, ibool* log_file_created, ulint k, ulint i)
{
	ibool	ret;
	ulint	arch_space_id;
	ulint	size;
	ulint	size_high;
	char	name[10000];

	UT_NOT_USED(create_new_db);

	*log_file_created = FALSE;

	/*获得log file的目录和文件路径*/
	srv_normalize_path_for_win(srv_log_group_home_dirs[k]);
	srv_log_group_home_dirs[k] = srv_add_path_separator_if_needed(srv_log_group_home_dirs[k]);
	sprintf(name, "%s%s%lu", srv_log_group_home_dirs[k], "ib_logfile", i);

	/*创建日志文件*/
	files[i] = os_file_create(name, OS_FILE_CREATE, OS_FILE_NORMAL, OS_LOG_FILE, &ret);
	if(!ret){ /*创建失败，可能日志文件已经存在*/
		if (os_file_get_last_error() != OS_FILE_ALREADY_EXISTS) {
			fprintf(stderr, "InnoDB: Error in creating or opening %s\n", name);
			return(DB_ERROR);
		}
		/*打开已经存在的文件*/
		files[i] = os_file_create(name, OS_FILE_OPEN, OS_FILE_AIO, OS_LOG_FILE, &ret);
		if (!ret) {
			fprintf(stderr, "InnoDB: Error in opening %s\n", name);
			return(DB_ERROR);
		}

		/*检查redo log file的大小是否与系统配置的大小一样*/
		ret = os_file_get_size(files[i], &size, &size_high);
		ut_a(ret);
		if (size != srv_calc_low32(srv_log_file_size) || size_high != srv_calc_high32(srv_log_file_size)) {
			fprintf(stderr,
				"InnoDB: Error: log file %s is of different size\n"
				"InnoDB: than specified in the .cnf file!\n", name);

			return(DB_ERROR);
		}
	}
	else{ /*redo log不存在，新建了一个新的log file*/
		*log_file_created = TRUE;

		ut_print_timestamp(stderr);

		fprintf(stderr, "  InnoDB: Log file %s did not exist: new to be created\n", name);
		fprintf(stderr, "InnoDB: Setting log file %s size to %lu MB\n", name, srv_log_file_size >> (20 - UNIV_PAGE_SIZE_SHIFT));
		fprintf(stderr, "InnoDB: Database physically writes the file full: wait...\n");
		/*设置文件占用的磁盘空间大小*/
		ret = os_file_set_size(name, files[i], srv_calc_low32(srv_log_file_size), srv_calc_high32(srv_log_file_size));
		if (!ret) {
			fprintf(stderr, "InnoDB: Error in creating %s: probably out of disk space\n", name);
			return(DB_ERROR);
		}
	}

	ret = os_file_close(files[i]);
	ut_a(ret);
	if(i == 0){
		/*建立一个log file的space对象*/
		fil_space_create(name, 2 * k + SRV_LOG_SPACE_FIRST_ID, FIL_LOG);
	}

	ut_a(fil_validate());
	/*将redo log加入到redo log space中*/
	fil_node_create(name, srv_log_file_size, 2 * k + SRV_LOG_SPACE_FIRST_ID);

	if (k == 0 && i == 0) {
		arch_space_id = 2 * k + 1 + SRV_LOG_SPACE_FIRST_ID;
		fil_space_create("arch_log_space", arch_space_id, FIL_LOG);
	} 
	else
		arch_space_id = ULINT_UNDEFINED;

	/*对redo log系统进行初始化*/
	if(i == 0)
		log_group_init(k, srv_n_log_files, srv_log_file_size * UNIV_PAGE_SIZE, 2 * k + SRV_LOG_SPACE_FIRST_ID, arch_space_id);

	return DB_SUCCESS;
}

/*创建或者打开数据库数据文件*/
static ulint open_or_create_data_files(ibool* create_new_db, dulint* min_flushed_lsn, ulint* min_arch_log_no, 
										dulint* max_flushed_lsn, ulint* max_arch_log_no, ulint* sum_of_new_sizes)
{
	ibool	ret;
	ulint	i;
	ibool	one_opened	= FALSE;
	ibool	one_created	= FALSE;
	ulint	size;
	ulint	size_high;
	ulint	rounded_size_pages;
	char	name[10000];

	if (srv_n_data_files >= 1000) {
		fprintf(stderr, "InnoDB: can only have < 1000 data files\n" "InnoDB: you have defined %lu\n", srv_n_data_files);
		return(DB_ERROR);
	}

	*sum_of_new_sizes = 0;
	*create_new_db = 0;

	srv_normalize_path_for_win(srv_data_home);
	srv_data_home = srv_add_path_separator_if_needed(srv_data_home);

	for(i = 0; i < srv_n_data_files; i ++){
		/*构建数据库文件的路径*/
		srv_normalize_path_for_win(srv_data_file_names[i]);
		sprintf(name, "%s%s", srv_data_home, srv_data_file_names[i]);

		/*尝试创建数据库文件*/
		files[i] = os_file_create(name, OS_FILE_CREATE, OS_FILE_NORMAL, OS_DATA_FILE, &ret);
		if(srv_data_file_is_raw_partition[i] == SRV_NEW_RAW){
			srv_created_new_raw = TRUE;
			/*创建失败，旧的文件还在*/
			files[i] = os_file_create(name, OS_FILE_OPEN, OS_FILE_NORMAL, OS_DATA_FILE, &ret);
			if (!ret) {
				fprintf(stderr, "InnoDB: Error in opening %s\n", name);
				return(DB_ERROR);
			}
		}
		else if(srv_data_file_is_raw_partition[i] == SRV_OLD_RAW)
			ret = FALSE;
		
		if(ret == FALSE){
			if (srv_data_file_is_raw_partition[i] != SRV_OLD_RAW && os_file_get_last_error() != OS_FILE_ALREADY_EXISTS) {
				fprintf(stderr, "InnoDB: Error in creating or opening %s\n", name);
				return(DB_ERROR);
			}

			if (one_created) {
				fprintf(stderr, "InnoDB: Error: data files can only be added at the end\n");
				fprintf(stderr, "InnoDB: of a tablespace, but data file %s existed beforehand.\n", name);
				return(DB_ERROR);
			}

			files[i] = os_file_create(name, OS_FILE_OPEN, OS_FILE_NORMAL, OS_DATA_FILE, &ret);
			if (!ret) {
				fprintf(stderr, "InnoDB: Error in opening %s\n", name);
				os_file_get_last_error();

				return(DB_ERROR);
			}

			if(srv_data_file_is_raw_partition[i] != SRV_OLD_RAW){ /*文件已经打开*/
				ret = os_file_get_size(files[i], &size, &size_high);
				ut_a(ret);
				/*校验数据库文件的长度*/
				rounded_size_pages = (size / (1024 * 1024) + 4096 * size_high) << (20 - UNIV_PAGE_SIZE_SHIFT); /*计算文件有多大，以page为单位计算*/
				if (i == srv_n_data_files - 1 && srv_auto_extend_last_data_file) {
					if (srv_data_file_sizes[i] > rounded_size_pages || (srv_last_file_size_max > 0 && srv_last_file_size_max < rounded_size_pages)) {
						fprintf(stderr,
							"InnoDB: Error: data file %s is of a different size\n"
							"InnoDB: than specified in the .cnf file!\n", name);	
					}
					srv_data_file_sizes[i] = rounded_size_pages;
				}

				if (rounded_size_pages != srv_data_file_sizes[i]) {
					fprintf(stderr, "InnoDB: Error: data file %s is of a different size\n"
						"InnoDB: than specified in the .cnf file!\n", name);
					return(DB_ERROR);
				}
			}
			/*从文件读取各种lsn和number,主要是check point位置*/
			fil_read_flushed_lsn_and_arch_log_no(files[i], one_opened, min_flushed_lsn, min_arch_log_no, max_flushed_lsn, max_arch_log_no);
			one_opened = TRUE;
		}
		else{ /*新创建的文件*/
			one_created = TRUE;
			if(i > 0){
				ut_print_timestamp(stderr);
				fprintf(stderr, "  InnoDB: Data file %s did not exist: new to be created\n", name);
			}
			else{
				fprintf(stderr, "InnoDB: The first specified data file %s did not exist:\n"
					"InnoDB: a new database to be created!\n", name);
				*create_new_db = TRUE;
			}
			ut_print_timestamp(stderr);
			fprintf(stderr, "  InnoDB: Setting file %s size to %lu MB\n", name, (srv_data_file_sizes[i] >> (20 - UNIV_PAGE_SIZE_SHIFT)));
			fprintf(stderr,"InnoDB: Database physically writes the file full: wait...\n");
			/*设置文件的大小，以page为单位*/
			ret = os_file_set_size(name, files[i], srv_calc_low32(srv_data_file_sizes[i]), srv_calc_high32(srv_data_file_sizes[i]));
			if (!ret) {
				fprintf(stderr, "InnoDB: Error in creating %s: probably out of disk space\n", name);
				return(DB_ERROR);
			}

			*sum_of_new_sizes = *sum_of_new_sizes + srv_data_file_sizes[i];
		}

		/*将文件关闭，并将文件加入到表空间(space node)中进行管理*/
		ret = os_file_close(files[i]);
		if (i == 0)
			fil_space_create(name, 0, FIL_TABLESPACE);
		ut_a(fil_validate());
		fil_node_create(name, srv_data_file_sizes[i], 0);
	}

	ios = 0;
	mutex_create(&ios_mutex);
	mutex_set_level(&ios_mutex, SYNC_NO_ORDER_CHECK);

	return DB_SUCCESS;
}

/*latch检测线程主体函数，对latches的竞争状态检测*/
static ulint test_measure_cont(void* arg)
{
	ulint	i, j;
	ulint	pcount, kcount, s_scount, s_xcount, s_mcount, lcount;

	fprintf(stderr, "Starting contention measurement\n");

	for (i = 0; i < 1000; i++) {
		pcount = 0;
		kcount = 0;
		s_scount = 0;
		s_xcount = 0;
		s_mcount = 0;
		lcount = 0;

		for (j = 0; j < 100; j++) {
			if (srv_measure_by_spin)
				ut_delay(ut_rnd_interval(0, 20000));
			else
				os_thread_sleep(20000);

			if (kernel_mutex.lock_word)
				kcount++;

			if (buf_pool->mutex.lock_word)
				pcount++;

			if (log_sys->mutex.lock_word)
				lcount++;

			if (btr_search_latch.reader_count)
				s_scount++;

			if (btr_search_latch.writer != RW_LOCK_NOT_LOCKED)
				s_xcount++;

			if (btr_search_latch.mutex.lock_word)
				s_mcount++;
		}

		fprintf(stderr, "Mutex res. l %lu, p %lu, k %lu s x %lu s s %lu s mut %lu of %lu\n", lcount, pcount, kcount, s_xcount, s_scount, s_mcount, j);
		fprintf(stderr, "log i/o %lu n non sea %lu n succ %lu n h fail %lu\n", log_sys->n_log_ios, btr_cur_n_non_sea, btr_search_n_succ, btr_search_n_hash_fail);
	}

	return 0;
}

/*innodb启动函数，创建或打开数据库*/
int innobase_start_or_create_for_mysql()
{
	ibool	create_new_db;
	ibool	log_file_created;
	ibool	log_created	= FALSE;
	ibool	log_opened	= FALSE;
	dulint	min_flushed_lsn;
	dulint	max_flushed_lsn;
	ulint	min_arch_log_no;
	ulint	max_arch_log_no;
	ibool	start_archive;
	ulint   sum_of_new_sizes;
	ulint	sum_of_data_file_sizes;
	ulint	tablespace_size_in_header;
	ulint	err;
	ulint	i;
	ulint	k;
	mtr_t   mtr;

	log_do_write = TRUE;

	srv_is_being_started = TRUE;
	srv_startup_is_before_trx_rollback_phase = TRUE;

	/*确定srv_unix_file_flush_method的值*/
	if (0 == ut_strcmp(srv_unix_file_flush_method_str, "fdatasync"))
		srv_unix_file_flush_method = SRV_UNIX_FDATASYNC;
	else if (0 == ut_strcmp(srv_unix_file_flush_method_str, "O_DSYNC"))
		srv_unix_file_flush_method = SRV_UNIX_O_DSYNC;
	else if (0 == ut_strcmp(srv_unix_file_flush_method_str, "littlesync"))
		srv_unix_file_flush_method = SRV_UNIX_LITTLESYNC;
	else if (0 == ut_strcmp(srv_unix_file_flush_method_str, "nosync"))
		srv_unix_file_flush_method = SRV_UNIX_NOSYNC;
	else{
		fprintf(stderr, "InnoDB: Unrecognized value %s for innodb_flush_method\n", srv_unix_file_flush_method_str);
		return(DB_ERROR);
	}

	os_aio_use_native_aio = srv_use_native_aio;

	err = srv_boot();
	if(err != DB_SUCCESS)
		return err;

	/*确定IO thread数量,最大100个*/
	if(srv_n_file_io_threads > SRV_MAX_N_IO_THREADS)
		srv_n_file_io_threads = SRV_MAX_N_IO_THREADS;

	/*用4条线程模拟异步IO*/
	os_aio_use_native_aio = FALSE;
	srv_n_file_io_threads = 4;

	os_aio_use_native_aio = FALSE;

	/*初始话AIO模块*/
	if (!os_aio_use_native_aio) /*模拟的AIO，segment数量是系统AIO的8倍？*/
		os_aio_init(8 * SRV_N_PENDING_IOS_PER_THREAD * srv_n_file_io_threads, srv_n_file_io_threads, SRV_MAX_N_PENDING_SYNC_IOS);
	else
		os_aio_init(SRV_N_PENDING_IOS_PER_THREAD * srv_n_file_io_threads, srv_n_file_io_threads, SRV_MAX_N_PENDING_SYNC_IOS);

	/*初始化表空间文件管理模块*/
	fil_init(SRV_MAX_N_OPEN_FILES);

	/*初始化buffer pool*/
	buf_pool_init(srv_pool_size, srv_pool_size);
	/*初始化表空间*/
	fsp_init();
	/*初始化redo log*/
	log_init();
	/*初始化事务所*/
	lock_sys_create(srv_lock_table_size);

	/*启动io线程*/
	for (i = 0; i < srv_n_file_io_threads; i++) {
		n[i] = i;
		os_thread_create(io_handler_thread, n + i, thread_ids + i);
	}

	if (0 != ut_strcmp(srv_log_group_home_dirs[0], srv_arch_dir)) {
		fprintf(stderr, "InnoDB: Error: you must set the log group home dir in my.cnf the\nInnoDB: same as log arch dir.\n");

		return(DB_ERROR);
	}
	/*对redo log files空间大小的判定，不能大于4G(262144个page, page size = 16KB,*/
	if (srv_n_log_files * srv_log_file_size >= 262144) {
		fprintf(stderr, "InnoDB: Error: combined size of log files must be < 4 GB\n");
		return(DB_ERROR);
	}

	sum_of_new_sizes = 0;
	for (i = 0; i < srv_n_data_files; i++) {
		if (sizeof(off_t) < 5 && srv_data_file_sizes[i] >= 262144) { /*32位UNIX系统，文件不能大于4G*/
			fprintf(stderr,"InnoDB: Error: file size must be < 4 GB with this MySQL binary\n"
				"InnoDB: and operating system combination, in some OS's < 2 GB\n");
			return(DB_ERROR);
		}
		sum_of_new_sizes += srv_data_file_sizes[i];
	}

	/*打开数据库文件*/
	err = open_or_create_data_files(&create_new_db, &min_flushed_lsn, &min_arch_log_no, &max_flushed_lsn, &max_arch_log_no, &sum_of_new_sizes);
	if(err != DB_SUCCESS){
		fprintf(stderr, "InnoDB: Could not open data files\n");
		return err;
	}

	/*通过double write缓冲的完整数据信息，恢复可能未写完整的page*/
	if (!create_new_db)
		trx_sys_doublewrite_restore_corrupt_pages();

	/*加载redo log file到fil space管理模块中*/
	srv_normalize_path_for_win(srv_arch_dir);
	srv_arch_dir = srv_add_path_separator_if_needed(srv_arch_dir);
	for(k = 0; k < srv_n_log_groups; k++){
		for (i = 0; i < srv_n_log_files; i++) {
			err = open_or_create_log_file(create_new_db, &log_file_created, k, i);
			if (err != DB_SUCCESS)
				return err;

			if (log_file_created) log_created = TRUE;
			else log_opened = TRUE;

			if ((log_opened && create_new_db) || (log_opened && log_created)) {
					fprintf(stderr, 
						"InnoDB: Error: all log files must be created at the same time.\n"
						"InnoDB: All log files must be created also in database creation.\n"
						"InnoDB: If you want bigger or smaller log files, shut down the\n"
						"InnoDB: database and make sure there were no errors in shutdown.\n"
						"InnoDB: Then delete the existing log files. Edit the .cnf file\n"
						"InnoDB: and start the database again.\n");

					return(DB_ERROR);
			}
		}
	}
	
	if (log_created && !create_new_db && !srv_archive_recovery) {
		if (ut_dulint_cmp(max_flushed_lsn, min_flushed_lsn) != 0 || max_arch_log_no != min_arch_log_no) {
			fprintf(stderr, 
				"InnoDB: Cannot initialize created log files because\n"
				"InnoDB: data files were not in sync with each other\n"
				"InnoDB: or the data files are corrupt.\n");

			return(DB_ERROR);
		}

		if (ut_dulint_cmp(max_flushed_lsn, ut_dulint_create(0, 1000)) < 0) {
			fprintf(stderr,
				"InnoDB: Cannot initialize created log files because\n"
				"InnoDB: data files are corrupt, or new data files were\n"
				"InnoDB: created when the database was started previous\n"
				"InnoDB: time but the database was not shut down\n"
				"InnoDB: normally after that.\n");

			return(DB_ERROR);
		}

		mutex_enter(&(log_sys->mutex));
		/*初始化redo log的位置和log_sys的所有LSN的位置,为日志推演做准备*/
		recv_reset_logs(max_flushed_lsn, max_arch_log_no + 1, TRUE);

		mutex_exit(&(log_sys->mutex));
	}

	/*初始化USER SESSION*/
	sess_sys_init_at_db_start();

	/*新创建的数据库*/
	if(create_new_db){
		mtr_start(&mtr);
		fsp_header_init(0, sum_of_new_sizes, &mtr);		
		mtr_commit(&mtr);

		trx_sys_create();
		dict_create();
		srv_startup_is_before_trx_rollback_phase = FALSE;
	}
	else if(srv_archive_recovery){ /*进行归档日志推演，主要用户数据恢复*/
		err = recv_recovery_from_archive_start(min_flushed_lsn, srv_archive_recovery_limit_lsn, min_arch_log_no);
		if (err != DB_SUCCESS)
			return(DB_ERROR);

		/* Since ibuf init is in dict_boot, and ibuf is needed in any disk i/o, first call dict_boot */
		dict_boot();
		/*初始化上一次数据库关闭前正在执行的事务对象*/
		trx_sys_init_at_db_start();
		/*上一次关闭前的事务未进行初始化之前，不能进行页预读*/
		srv_startup_is_before_trx_rollback_phase = FALSE;

		/* Initialize the fsp free limit global variable in the log system */
		fsp_header_get_free_limit(0);
		recv_recovery_from_archive_finish();
	}
	else{ /*从正常的checkpoint处进行redo log推演*/
		err = recv_recovery_from_checkpoint_start(LOG_CHECKPOINT, ut_dulint_max, min_flushed_lsn, max_flushed_lsn);
		if(err != DB_SUCCESS)
			return DB_ERROR;

		dict_boot();
		trx_sys_init_at_db_start();
		/*上一次关闭前的事务未进行初始化之前，不能进行页预读*/
		srv_startup_is_before_trx_rollback_phase = FALSE;
		fsp_header_get_free_limit(0);
		/*完成日志推演，并回滚掉所有上次服务关闭之前未完成的事务*/
		recv_recovery_from_checkpoint_finish();
	}

	if(!create_new_db && sum_of_new_sizes > 0){
		mtr_start(&mtr);
		/*初始化系统空间表的page数量,疑问？！*/
		fsp_header_inc_size(0, sum_of_new_sizes, &mtr);		
		mtr_commit(&mtr);
	}

	if(recv_needed_recovery){
		ut_print_timestamp(stderr);
		fprintf(stderr, " InnoDB: Flushing modified pages from the buffer pool...\n");
	}
	/*建立一个checkpoint,因为前面redo log进行了日志推演*/
	log_make_checkpoint_at(ut_dulint_max, TRUE);
	if (!srv_log_archive_on)
		ut_a(DB_SUCCESS == log_archive_noarchivelog());
	else {
		mutex_enter(&(log_sys->mutex));

		start_archive = FALSE;

		if (log_sys->archiving_state == LOG_ARCH_OFF)
			start_archive = TRUE;

		mutex_exit(&(log_sys->mutex));

		if (start_archive)
			ut_a(DB_SUCCESS == log_archive_archivelog());
	}

	/*对latch竞争状态的检测*/
	if(srv_measure_contention){
		/* os_thread_create(&test_measure_cont, NULL, thread_ids + SRV_MAX_N_IO_THREADS); */
	}
	/*建立事务lock超时检测的线程*/
	os_thread_create(&srv_lock_timeout_and_monitor_thread, NULL, thread_ids + 2 + SRV_MAX_N_IO_THREADS);	
	/*建立错误监控线程*/
	os_thread_create(&srv_error_monitor_thread, NULL, thread_ids + 3 + SRV_MAX_N_IO_THREADS);

	srv_was_started = TRUE;
	srv_is_being_started = FALSE;
	sync_order_checks_on = TRUE;

	if (srv_use_doublewrite_buf && trx_doublewrite == NULL) /*如果doublewrite没有被建立，建立一个doublewrite缓冲区*/
		trx_sys_create_doublewrite_buf();

	/*创建外键约束*/
	err = dict_create_or_check_foreign_constraint_tables();
	if (err != DB_SUCCESS)
		return((int)DB_ERROR);

	/*创建master线程*/
	os_thread_create(&srv_master_thread, NULL, thread_ids + 1 + SRV_MAX_N_IO_THREADS);

	sum_of_data_file_sizes = 0;
	for (i = 0; i < srv_n_data_files; i++) {
		sum_of_data_file_sizes += srv_data_file_sizes[i];
	}

	tablespace_size_in_header = fsp_header_get_tablespace_size(0);

	if (!srv_auto_extend_last_data_file && sum_of_data_file_sizes != tablespace_size_in_header) {
		fprintf(stderr,
			"InnoDB: Error: tablespace size stored in header is %lu pages, but\n"
			"InnoDB: the sum of data file sizes is %lu pages\n",
			tablespace_size_in_header, sum_of_data_file_sizes);
	}

	if (srv_auto_extend_last_data_file && sum_of_data_file_sizes < tablespace_size_in_header) {
			fprintf(stderr,
				"InnoDB: Error: tablespace size stored in header is %lu pages, but\n"
				"InnoDB: the sum of data file sizes is only %lu pages\n",
				tablespace_size_in_header, sum_of_data_file_sizes);
	}

	/*测试os pthread mutex*/
	os_fast_mutex_init(&srv_os_test_mutex);

	if (0 != os_fast_mutex_trylock(&srv_os_test_mutex)) {
		fprintf(stderr, "InnoDB: Error: pthread_mutex_trylock returns an unexpected value on\n"
			"InnoDB: success! Cannot continue.\n");
		exit(1);
	}

	os_fast_mutex_unlock(&srv_os_test_mutex);
	os_fast_mutex_lock(&srv_os_test_mutex);
	os_fast_mutex_unlock(&srv_os_test_mutex);

	ut_print_timestamp(stderr);
	fprintf(stderr, "  InnoDB: Started\n");

	return((int) DB_SUCCESS);
}

/*正常关闭innodb*/
int innobase_shutdown_for_mysql()
{
	if (!srv_was_started) {
		if (srv_is_being_started) {
			ut_print_timestamp(stderr);
			fprintf(stderr, "  InnoDB: Warning: shutting down a not properly started\n");
			fprintf(stderr, "InnoDB: or created database!\n");
		}

		return(DB_SUCCESS);
	}
	/*建议log刷盘，建立一个checkpoint*/
	logs_empty_and_mark_files_at_shutdown();

	if (srv_conc_n_threads != 0) {
		fprintf(stderr, "InnoDB: Warning: query counter shows %ld queries still\n"
			"InnoDB: inside InnoDB at shutdown\n",
			srv_conc_n_threads);
	}
	/*释放内存池*/
	ut_free_all_mem();

	return DB_SUCCESS;
}

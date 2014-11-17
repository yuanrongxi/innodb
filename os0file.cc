#include "os0file.h"
#include "os0sync.h"
#include "os0thread.h"
#include "ut0mem.h"
#include "srv0srv.h"
#include "fil0fil.h"
#include "buf0buf.h"

#undef HAVE_FDATASYNC

/*文件的seek mutex个数*/
#define OS_FILE_N_SEEK_MUTEXES 16
os_mutex_t	os_file_seek_mutexes[OS_FILE_N_SEEK_MUTEXES];

ulint	os_innodb_umask	= 0;
/*默认每次写会触发flush,假如在double write机制启动后，会设置成TRUE*/
ibool	os_do_not_call_flush_at_each_write = FALSE;

#define OS_AIO_MERGE_N_CONSECUTIVE	64

/*默认不使用native aio*/
ibool	os_aio_use_native_aio = FALSE;

ibool	os_aio_print_debug = FALSE;

typedef struct os_aio_slot_struct
{
	ibool			is_read;			/*是否是读操作*/
	ulint			pos;				/*slot array的索引位置*/
	
	ibool			reserved;			/*这个slot是否被占用了*/
	ulint			len;				/*读写的块长度*/

	byte*			buf;				/**/
	ulint			type;				/*操作类型：OS_FILE_READ OS_FILE_WRITE*/
	ulint			offset;				/*当前操作文件偏移位置，低32位*/
	ulint			offset_high;		/*当前操作文件偏移位置，高32位*/

	os_file_t		file;				/*文件句柄*/
	char*			name;				/*文件名*/
	ibool			io_already_done;	/*在模拟aio的模式下使用，TODO*/
	void*			message1;
	void*			message2;

#ifdef POSIX_ASYNC_IO
	struct aiocb	control;				/*posix 控制块*/
#endif
}os_aio_slot_t;

/*slots array结构定义*/
typedef struct os_aio_array_struct
{
	os_mutex_t		mutex;		/*slots array的互斥锁*/
	os_event_t		not_full;	/*可以插入数据的信号*/
	os_event_t		is_empty;	/**/

	ulint			n_slots;	/*slots总体单元个数*/
	ulint			n_segments; /**/
	ulint			n_reserved; /*被占用的slots个数*/
	os_aio_slot_t*	slots;		/*slots数组*/

	os_event_t*		event;		/*slots event array*/
}os_aio_array_t;

os_event_t* os_aio_segment_wait_events = NULL;

/*读线程的aio slots array*/
os_aio_array_t*	os_aio_read_array	= NULL;
/*写线程的aio slots array*/
os_aio_array_t*	os_aio_write_array	= NULL;
/*insert buffer的slots array*/
os_aio_array_t*	os_aio_ibuf_array	= NULL;
/*log线程的aio slots array*/
os_aio_array_t*	os_aio_log_array	= NULL;
os_aio_array_t*	os_aio_sync_array	= NULL;

ulint os_aio_n_segments = ULINT_UNDEFINED;

ibool os_aio_recommend_sleep_for_read_threads = FALSE;

/*一些IO的统计信息*/
ulint	os_n_file_reads					= 0; /*磁盘读取的次数*/
ulint	os_bytes_read_since_printout	= 0;
ulint	os_n_file_writes				= 0;
ulint	os_n_fsyncs						= 0;
ulint	os_n_file_reads_old				= 0;
ulint	os_n_file_writes_old			= 0;
ulint	os_n_fsyncs_old					= 0;

time_t	os_last_printout;

ibool	os_has_said_disk_full			= FALSE;

/*只对WINDOWS有效，呵呵，这里不做任何实现，WINDOWS忽略*/
ulint os_get_os_version()
{
	ut_error;
	return(0);
}

ulint os_file_get_last_error()
{
	ulint err = (ulint)errno;
	if(err != EEXIST && err != ENOSPC){
		ut_print_timestamp(stderr);

		fprintf(stderr, "  InnoDB: Operating system error number %li in a file operation.\n" 
			"InnoDB: See http://www.innodb.com/ibman.html for installation help.\n",
			(long) err);

		if (err == ENOENT){
			fprintf(stderr, "InnoDB: The error means the system cannot find the path specified.\n"
				"InnoDB: In installation you must create directories yourself, InnoDB\n"
				"InnoDB: does not create them.\n");
		} 
		else if (err == EACCES){
			fprintf(stderr, "InnoDB: The error means mysqld does not have the access rights to\n"
				"InnoDB: the directory.\n");
		} 
		else {
			fprintf(stderr, "InnoDB: Look from section 13.2 at http://www.innodb.com/ibman.html\n"
				"InnoDB: what the error number means or use the perror program of MySQL.\n");
		}
	}

	if(err == ENOSPC)
		return OS_FILE_DISK_FULL;
#ifdef POSIX_ASYNC_IO
	else if (err == EAGAIN) {
		return OS_FILE_AIO_RESOURCES_RESERVED;
#endif
	}
	else if(err == EAGAIN)
		return OS_FILE_AIO_RESOURCES_RESERVED;
	else if(err == ENOENT)
		return OS_FILE_NOT_FOUND;
	else if(err == EEXIST)
		return OS_FILE_ALREADY_EXISTS;
	else
		return 100 + err;

}

static ibool os_file_handle_error(os_file_t file, char* name)
{
	ulint err;

	UT_NOT_USED(file);

	err = os_file_get_last_error();
	if(err == OS_FILE_DISK_FULL){ /*磁盘满了,只打印一次*/
		if(os_has_said_disk_full)
			return FALSE;

		if(name != NULL){
			ut_print_timestamp(stderr);
			fprintf(stderr, "  InnoDB: Encountered a problem with file %s\n", name);
		}

		ut_print_timestamp(stderr);
		fprintf(stderr, "  InnoDB: Disk is full. Try to clean the disk to free space.\n");

		os_has_said_disk_full = TRUE;

		return FALSE;
	}
	else if(err == OS_FILE_AIO_RESOURCES_RESERVED)
		return TRUE;
	else if(err == OS_FILE_ALREADY_EXISTS)
		return FALSE;
	else{
		if (name != NULL)
			fprintf(stderr, "InnoDB: File name %s\n", name);
		/*严重错误，直接停掉了mysqld*/
		fprintf(stderr, "InnoDB: Cannot continue operation.\n");
		exit(1);
	}

	return FALSE;
}

void os_io_init_simple()
{
	ulint i;
	for(i = 0; i < OS_FILE_N_SEEK_MUTEXES; i++)
		os_file_seek_mutexes[i] = os_mutex_create(NULL);
}

os_file_t os_file_create_simple(char* name, ulint create_mode, ulint access_type, ibool* success)
{
	os_file_t	file;
	int			create_flag;
	ibool		retry;

try_again:
	ut_a(name);

	/*文件打开*/
	if(create_mode == OS_FILE_OPEN){
		if(access_type == OS_FILE_READ_ONLY)
			create_flag = O_RDONLY;
		else
			create_flag = O_RDWR;
	}
	else if(create_mode == OS_FILE_CREATE)/*新建一个文件*/
		create_flag = O_RDWR | O_CREAT | O_EXCL;
	else{
		create_flag = 0;
		ut_error;
	}

	if(file == -1){
		*success = FALSE;
		retry = os_file_handle_error(file, name); /*获得磁盘操作错误码，并判断是否可以继续重试*/
		if(retry)
			goto try_again;
	}
	else
		*success = TRUE;

	return file;
}

os_file_t os_file_create(char* name, ulint create_mode, ulint purpose, ulint type, ibool* success)
{
	os_file_t	file;
	int			create_flag;
	ibool		retry;

try_again:
	ut_a(name);
	
	if(create_mode == OS_FILE_OPEN)
		create_flag = O_RDWR;
	else if(create_mode == OS_FILE_CREATE)
		create_flag = O_RDWR | O_CREAT | O_EXCL;
	else if(create_mode == OS_FILE_OVERWRITE)
		create_flag = O_RDWR | O_CREAT | O_TRUNC;
	else{
		create_flag = 0;
		ut_error;
	}

	UT_NOT_USED(purpose);

#ifdef O_SYNC
	/*不支持二次写入且 srv_unix_file_flush_method是SRV_UNIX_O_DSYNC，必须设置成同步写入方式*/
	if ((!srv_use_doublewrite_buf || type != OS_DATA_FILE) && srv_unix_file_flush_method == SRV_UNIX_O_DSYNC) {
			create_flag = create_flag | O_SYNC;
	}
#endif

	if(create_mode == OF_FILE_CREATE)
		file = open(name, create_flag, os_innodb_umask);
	else
		file = open(name, create_flag);

	if(file == -1){
		*success = FALSE;
		retry = os_file_handle_error(file, name);
		if(retry)
			goto try_again;
	}
	else
		*success = TRUE;

	return file;
}

ibool os_file_close(os_file_t file)
{
	int ret = close(file);
	if(ret == -1){
		os_file_handle_error(file, NULL);
		return(FALSE);
	}
	return TRUE;
}

ibool os_file_get_size(os_file_t file, ulint* size, ulint* size_high)
{
	off_t offs = lseek(file, 0, SEEK_END);
	if(offs == ((off_t)-1))
		return FALSE;

	if(sizeof(off_t) > 4){ 
		*size = (ulint)(offs & 0xFFFFFFFF); /*获得低位*/
		*size_high = (ulint)(offs >> 32);   /*获得高位*/
	}
	else{
		*size = (ulint) offs;
		*size_high = 0;
	}
}

ibool os_file_set_size(char* name, os_file_t file, ulint size, ulint size_high)
{
	ib_longlong	offset;
	ib_longlong	low;
	ulint   	n_bytes;
	ibool		ret;
	byte*   	buf;
	byte*   	buf2;
	ulint   	i;

	ut_a(size == (size & 0xFFFFFFFF));
	/*用8M作为写入缓冲区，linux最大的fsync 1M*/
	buf2 = ut_malloc(UNIV_PAGE_SIZE * 513);
	buf2 = ut_align(buf2, UNIV_PAGE_SIZE);

	for(i = 0; i < UNIV_PAGE_SIZE; i ++)
		buf[i] = '\0';

	offset = 0;
	/*确定文件的长度*/
	low = (ib_longlong)size + (((ib_longlong)size_high) << 32);
	while(offset < low){
		if (low - offset < UNIV_PAGE_SIZE * 512) /*小于8M*/
			n_bytes = (ulint)(low - offset);
		else /*大于8M的差距，直接设置成8M写入*/
			n_bytes = UNIV_PAGE_SIZE * 512;

		ret = os_file_write(name, file, buf, (ulint)(offset & 0xFFFFFFFF), (ulint)(offset >> 32), n_bytes);
		if(!ret){
			ut_free(buf2);
			goto error_handling;
		}

		/*更改偏移量*/
		offset += n_bytes;
	}

	ut_free(buf2);
	ret = os_file_flush(file);
	if(ret)
		return TRUE;

error_handling:
	return FALSE;
}

ibool os_file_flush(os_file_t file)
{
	int ret;

#ifdef HAVE_FDATASYNC
	ret = fdatasync(file);
#else
	ret = fsync(file);
#endif

	os_n_fsyncs ++;
	if(ret == 0)
		return TRUE;

	if(errno == EINVAL)
		return TRUE;

	ut_print_timestamp(stderr);
	fprintf(stderr, "  InnoDB: Error: the OS said file flush did not succeed\n");

	os_file_handle_error(file, NULL);
	ut_a(0);

	return FALSE;
}

static ssize_t os_file_pread(os_file_t file, void* buf, ulint n, ulint offset, ulint offset_high)
{
	/*offset < 4G*/
	ut_a((offset & 0xFFFFFFFF) == offset);
	if (sizeof(off_t) > 4)
		offs = (off_t)offset + (((off_t)offset_high) << 32);
	else { /*off_t最大支持4G的时候，如果ooffset_high > 0，表示读异常*/
		offs = (off_t)offset;
		if (offset_high > 0)
			fprintf(stderr, "InnoDB: Error: file read at offset > 4 GB\n");
	}

	os_n_file_reads ++;

#ifdef HAVE_PREAD
	return(pread(file, buf, n, offs));
#else /*用lseek和read组合读取*/
	ssize_t	ret;
	ulint	i;
	/* Protect the seek / read operation with a mutex */
	i = ((ulint) file) % OS_FILE_N_SEEK_MUTEXES; /*获得对应的mutex*/

	os_mutex_enter(os_file_seek_mutexes[i]);
	ret = lseek(file, offs, 0);
	if (ret < 0) {
		os_mutex_exit(os_file_seek_mutexes[i]);
		return(ret);
	}

	ret = read(file, buf, n);
	os_mutex_exit(os_file_seek_mutexes[i]);

	return(ret);
#endif
}

static SSIZE_T os_file_pwrite(os_file_t file, void* buf, ulint n, ulint offset, ulint offset_high)
{
	ssize_t	ret;
	off_t	offs;

	ut_a((offset & 0xFFFFFFFF) == offset);

	if (sizeof(off_t) > 4)
		offs = (off_t)offset + (((off_t)offset_high) << 32);
	else{
		offs = (off_t)offset;

		if (offset_high > 0)
			fprintf(stderr, "InnoDB: Error: file write at offset > 4 GB\n");
	}

	os_n_file_writes ++;

#ifdef HAVE_PWRITE
	ret = pwrite(file, buf, n, offs);
	/*判断是否需要立即fsync*/
	if((srv_unix_file_flush_method != SRV_UNIX_LITTLESYNC && srv_unix_file_flush_method != SRV_UNIX_NOSYNC && !os_do_not_call_flush_at_each_write)){
		ut_a(TRUE == os_file_flush(file));
	}
	
	return ret;
#else
	{
		ulint i;
		i = ((ulint) file) % OS_FILE_N_SEEK_MUTEXES;
		os_mutex_enter(os_file_seek_mutexes[i]);

		ret = lseek(file, offs, 0);
		if(ret < 0){
			os_mutex_exit(os_file_seek_mutexes[i]);
			return(ret);
		}

		ret = write(file, buf, n);
		if (srv_unix_file_flush_method != SRV_UNIX_LITTLESYNC
			&& srv_unix_file_flush_method != SRV_UNIX_NOSYNC
			&& !os_do_not_call_flush_at_each_write){
				ut_a(TRUE == os_file_flush(file));
		}

		os_mutex_exit(os_file_seek_mutexes[i]);
		return ret;
	}
#endif
}

ibool os_file_read(os_file_t file, void* buf, ulint offset, ulint offset_high, ulint n)
{
	ibool retry;
	ssize_t ret;

	os_bytes_read_since_printout += n;

try_again:
	ret = os_file_pread(file, buf, n, offset, offset_high);
	if((ulint)ret == n)
		return TRUE;

	retry = os_file_handle_error(file, NULL); 
	if(retry)
		goto try_again;
	
	ut_error;

	return FALSE;
}

ibool os_file_write(char* name, os_file_t file, void* buf, ulint offset, ulint offset_high, ulint n)
{
	ssize_t ret;
	ret = os_file_pwrite(file, buf, n, offset, offset_high);
	if((ulint) ret = n)
		return TRUE;

	if(!os_has_said_disk_full){
		ut_print_timestamp(stderr);

		fprintf(stderr,
			"  InnoDB: Error: Write to file %s failed at offset %lu %lu.\n"
			"InnoDB: %lu bytes should have been written, only %ld were written.\n"
			"InnoDB: Operating system error number %lu.\n"
			"InnoDB: Look from section 13.2 at http://www.innodb.com/ibman.html\n"
			"InnoDB: what the error number means or use the perror program of MySQL.\n"
			"InnoDB: Check that your OS and file system support files of this size.\n"
			"InnoDB: Check also that the disk is not full or a disk quota exceeded.\n",
			name, offset_high, offset, n, (long int)ret, (ulint)errno);
		os_has_said_disk_full = TRUE;
	}

	return FALSE;
}

static os_aio_slot_t* os_aio_array_get_nth_slot(os_aio_array_t* array, ulint index)
{
	ut_a(index < array->n_slots);
	return ((array->slots) + index);
}

static os_aio_array_t* os_aio_array_create(ulint n, ulint n_segments)
{
	os_aio_array_t* array;
	ulint			i;
	os_aio_slot_t*	slot;

	ut_a(n > 0);
	ut_a(n_segments > 0);
	ut_a(n % n_segments == 0);

	array = ut_malloc(sizeof(os_aio_array_t));
	array->mutex = os_mutex_create(NULL);
	array->not_full = os_cra
}











#include "sync0arr.h"
#include "sync0sync.h"
#include "os0sync.h"
#include "srv0srv.h"

struct sync_cell_struct
{
	void*			wait_object;
	
	mutex_t*		old_wait_mutex;
	rw_lock_t*		old_wait_lock;
	
	ulint			request_type;		/*lock type*/
	
	char*			file;
	ulint			line;
	os_thread_id_t	thread;				/*等待的线程ID*/

	ibool			waiting;			/*thread调用*/

	ibool			event_set;			
	os_event_t		evet;				/*本cell的信号*/
	time_t			reservation_time;
};

struct sync_array_struct
{
	ulint			n_reserved;		/*使用的cell个数*/
	ulint			n_cells;		/*array总个数*/
	sync_cell_t*	array;

	ulint			protection;		/*互斥类型，如果SYNC_ARRAY_OS_MUTEX用os mutex，否则用mutex*/
	mutex_t			mutex;
	os_mutex_t		os_mutex;

	ulint			sg_count;
	ulint			res_count;
};

/*检测死锁*/
static ibool sync_array_detect_deadlock(sync_array_t* arr, sync_cell_t* start, sync_cell_t* cell, ulint depth);

static sync_cell_t* sync_array_get_nth_cell(sync_array_t* arr, ulint n)
{
	ut_a(arr);
	ut_a(n < arr->n_cells);
	return (arr->array + n);
}

static void sync_array_enter(sync_array_t* arr)
{
	ulint protection = arr->protection;
	if(protection ==SYNC_ARRAY_OS_MUTEX)		/*OS mutex*/
		os_mutex_enter(arr->os_mutex);	
	else if(protection == SYNC_ARRAY_MUTEX)		/*sync mutex*/
		mutex_enter(&(arr->mutex));
	else
		ut_error;
}

static void sync_array_exit(sync_array_t* arr)
{
	ulint protection = arr->protection;
	if (protection == SYNC_ARRAY_OS_MUTEX) 
		os_mutex_exit(arr->os_mutex);
	else if(protection == SYNC_ARRAY_MUTEX)
		mutex_exit(&(arr->mutex));
	else
		ut_error;
}

sync_array_t* sync_array_create(ulint n_cells, ulint protection)
{
	sync_array_t*	arr;
	sync_cell_t*	cell_array;
	sync_cell_t*	cell;
	ulint			i;

	ut_a(n_cells > 0);
	/*分配一个sync_array_t结构内存*/
	arr = ut_malloc(sizeof(sync_array_t));
	/*分配一个n_cells长度的cell数组*/
	cell_array = ut_malloc(sizeof(sync_cell_t) * n_cells);
	arr->n_cells = n_cells;
	arr->protection = protection;
	arr->n_reserved = 0;
	arr->array = cell_array;
	arr->sg_count = 0;
	arr->res_count = 0;

	/*创建互斥量*/
	if(protection == SYNC_ARRAY_OS_MUTEX)
		arr->os_mutex = os_mutex_create(NULL);
	else if(protection == SYNC_ARRAY_MUTEX){
		mutex_create(&(arr->mutex));
		mutex_set_level(&(arr->mutex), SYNC_NO_ORDER_CHECK);
	}
	else{
		ut_error;
	}

	/*对cell的初始化*/
	for(i = 0; i < n_cells; i++){ 
		cell = sync_array_get_nth_cell(arr, i);
		cell->wait_object = NULL;
		cell->event_ = os_event_create(NULL);
		cell->event_set = FALSE;
	}

	return arr;
}

void sync_array_free(sync_array_t* arr)
{
	ulint			i;
	sync_cell_t*	cell;
	ulint			protection;

	ut_a(arr->n_reserved == 0);

	/*校验合法性*/
	sync_array_validate(arr);
	for(i = 0; i < arr->n_cells; i++){
		cell = sync_array_get_nth_cell(arr, i);
		/*释放信号量*/
		os_event_free(cell->event);
	}

	protection = arr->protection;
	switch(protection){
	case SYNC_ARRAY_OS_MUTEX:
		os_mutex_free(arr->os_mutex);
		break;

	case SYNC_ARRAY_MUTEX:
		mutex_free(&(arr->os_mutex));
		break;

	default:
		ut_error;
	}

	/*释放数组和结构内存*/
	ut_free(arr->array);
	ut_free(arr);
}

void sync_array_validate(sync_array_t* arr)
{
	ulint			i;
	sync_cell_t*	cell;
	ulint			count = 0;

	sync_array_enter(arr);
	for(i = 0; i < arr->n_cells; i ++){
		cell = sync_array_get_nth_cell(arr, i);
		if(cell->wait_object != NULL)
			count ++;
	}

	/*检验count是否和n_reserved相等*/
	ut_a(count == arr->n_reserved);

	sync_array_exit(arr);
}

static void sync_cell_event_set(sync_cell_t* cell)
{
	os_event_set(cell->event);
	cell->event_set = TRUE;
}

static void sync_cell_event_reset(sync_cell_t* cell)
{
	os_event_reset(cell->event);
	cell->event_set = FALSE;
}

void sync_array_reserve_cell(sync_array_t* arr, void* object, ulint type, char* file, ulint line, ulint* index)
{
	sync_cell_t*   	cell;
	ulint           i;

	ut_a(object);
	ut_a(index);

	sync_array_enter(arr);
	arr->res_count ++;

	for(i = 0; i < arr->n_cells; i++){
		cell = sync_array_get_nth_cell(arr, i);  
		if(cell->wait_object == NULL){ /*空闲的cell*/

			/*复位信号量*/
			if(cell->event_set)
				sync_cell_event_reset(cell);
			/*设置cell内容*/
			cell->reservation_time = time_t(NULL);
			cell->thread = os_thread_get_curr_id();
			cell->wait_object = object;
			if(type == SYNC_MUTEX) /*判断latch的类型*/
				cell->old_wait_mutex = object;
			else
				cell->old_wait_rw_lock = object;

			cell->request_type = type;
			cell->waiting = FALSE;
			cell->file = file;
			cell->line = line;
			
			arr->n_reserved ++;
			*index = i;
			sync_array_exit(arr);

			return ;
		}
	}

	ut_error;
}

void sync_array_wait_event(sync_array_t* arr, ulint index)
{
	sync_cell_t*	cell;
	os_event_t		event;

	sync_array_enter(arr);
	cell = sync_array_get_nth_cell(arr, index);

	ut_a(cell->wait_object);
	ut_a(!cell->waiting);
	ut_ad(os_thread_get_curr_id() == cell->thread);

	event = cell->event;
	cell->waiting = TRUE;

	sync_array_exit(arr);
	/*进行等待*/
	os_event_wait(event);
	sync_array_free_cell(arr, index);
}

static void sync_array_cell_print(char* buf, sync_cell_t* cell)
{
	mutex_t*	mutex;
	rw_lock_t*	rwlock;
	char*		str	 = NULL;
	ulint		type;

	type = cell->request_type;

	buf += sprintf(buf, "--Thread %lu has waited at %s line %lu for %.2f seconds the semaphore:\n",
			os_thread_pf(cell->thread), cell->file, cell->line,
			difftime(time(NULL), cell->reservation_time));

	if (type == SYNC_MUTEX) {
		/* We use old_wait_mutex in case the cell has already
		been freed meanwhile */
		mutex = cell->old_wait_mutex;

		buf += sprintf(buf,
		"Mutex at %lx created file %s line %lu, lock var %lu\n",
			(ulint)mutex, mutex->cfile_name, mutex->cline,
							mutex->lock_word);
		buf += sprintf(buf,
		"Last time reserved in file %s line %lu, waiters flag %lu\n",
			mutex->file_name, mutex->line, mutex->waiters);

	} else if (type == RW_LOCK_EX || type == RW_LOCK_SHARED) {

		if (type == RW_LOCK_EX) {
			buf += sprintf(buf, "X-lock on");
		} else {
			buf += sprintf(buf, "S-lock on");
		}

		rwlock = cell->old_wait_rw_lock;

		buf += sprintf(buf,
			" RW-latch at %lx created in file %s line %lu\n",
			(ulint)rwlock, rwlock->cfile_name, rwlock->cline);
		if (rwlock->writer != RW_LOCK_NOT_LOCKED) {
			buf += sprintf(buf,
			"a writer (thread id %lu) has reserved it in mode",
				os_thread_pf(rwlock->writer_thread));
			if (rwlock->writer == RW_LOCK_EX) {
				buf += sprintf(buf, " exclusive\n");
			} else {
				buf += sprintf(buf, " wait exclusive\n");
 			}
		}
		
		buf += sprintf(buf,
				"number of readers %lu, waiters flag %lu\n",
				rwlock->reader_count, rwlock->waiters);
	
		buf += sprintf(buf,
				"Last time read locked in file %s line %lu\n",
			rwlock->last_s_file_name, rwlock->last_s_line);
		buf += sprintf(buf,
			"Last time write locked in file %s line %lu\n",
			rwlock->last_x_file_name, rwlock->last_x_line);
	} else {
		ut_error;
	}

        if (!cell->waiting) {
          	buf += sprintf(buf, "wait has ended\n");
	}

        if (cell->event_set) {
             	buf += sprintf(buf, "wait is ending\n");
	}
}

/*通过thread id找到对应array cell*/
static sync_cell_t* sync_array_find_thread(sync_array_t* arr, os_thread_id_t* thread)
{
	ulint			i;
	sync_cell_t*	cell;

	for(i = 0; i < arr->n_cells; i++){
		cell = sync_array_get_nth_cell(arr, i);
		if(cell->wait_object != NULL && os_thread_eq(cell->thread, thread))
			return cell;
	}

	return NULL;
}

/*死锁判断*/
static ibool sync_array_deadlock_step(sync_array_t* arr, sync_cell_t* start, os_thread_id_t thread, ulint pass, ulint depth)
{
	sync_cell_t* new;
	ibool ret;

	depth ++;
	
	if(pass != 0)
		return FALSE;

	new = sync_array_find_thread(arr, thread);
	if(new == start){
		ut_dbg_stop_threads = TRUE;
		/*死锁*/
		printf("########################################\n");
		printf("DEADLOCK of threads detected!\n");

		return TRUE;
	}
	else if(new != NULL){
		ret = sync_array_detect_deadlock(arr, start, new, depth);
		if(ret)
			return TRUE;
	}

	return FALSE;
}

/*死锁检测*/
static ibool sync_array_detect_deadlock(sync_array_t* arr, sync_cell_t* start, sync_cell_t* cell, ulint depth)
{
	mutex_t*			mutex;
	rw_lock_t*			lock;
	os_thread_id_t		thread;
	ibool				ret;
	rw_lock_debug_t*	debug;
	char				buf[500];

	ut_a(arr && start && cell);
	ut_ad(cell->wait_object);
	ut_ad(os_thread_get_curr_id() == start->thread);
	ut_ad(depth);

	depth ++;
	if(cell->event_set || !cell->waiting) /*这个cell无锁正在等待*/
		return FALSE;

	if(cell->request_type == SYNC_MUTEX){
		mutex = cell->wait_object;
		if(mutex_get_lock_word(mutex) != 0){ /*这个cell不能获得锁，正在等待中*/
			thread = mutex->thread_id;
			/*判断它等待获取的这个锁被那个线程获取了，从而得出获取锁的线程是不是也在等待，这里用了交叉递归的方式做判断*/
			ret = sync_array_deadlock_step(arr, start, thread, 0, depth);
			if(ret){ /*死锁了，打印死锁的日志*/
				sync_array_cell_print(buf, cell);
				printf("Mutex %lx owned by thread %lu file %s line %lu\n%s", (ulint)mutex, os_thread_pf(mutex->thread_id),
					mutex->file_name, mutex->line, buf);

				return TRUE;
			}
		}

		return FALSE;
	}
	else if(cell->request_type == RW_LOCK_EX){ /*是一个rw_lock X-latch*/
		lock = cell->wait_object;
		debug = UT_LIST_GET_FIRST(lock->debug_list);
		while(debug != NULL){
			/*独占锁不能与任何rw_lock兼容，其中包括非同一线程的x-latch和x-wait-latch、S-latch*/
			if(((debug->lock_type == RW_LOCK_EX) && !os_thread_eq(thread, cell->thread))
				|| ((debug->lock_type == RW_LOCK_WAIT_EX)&& !os_thread_eq(thread, cell->thread))
				|| (debug->lock_type == RW_LOCK_SHARED)){
					ret = sync_array_deadlock_step(arr, start, thread, debug->pass, depth);
					if(ret){
						sync_array_cell_print(buf, cell);
						printf("rw-lock %lx %s ", (ulint) lock, buf);
						rw_lock_debug_print(debug);

						return(TRUE);
					}
			}
			debug = UT_LIST_GET_NEXT(list, debug);
		}
		return FALSE;
	}
	else if(cell->request_type == RW_LOCK_SHARED){ /*是rw_lock S-latch*/
		lock = cell->wait_object;
		debug = UT_LIST_GET_FIRST(lock->debug_list);
		while(debug != NULL){
			thread = debug->thread_id;
			/*S-latch不能和任何X-latch兼容，只和S-latch兼容*/
			if(debug->lock_type == RW_LOCK_EX || debug->lock_type == RW_LOCK_WAIT_EX){
				ret = sync_array_deadlock_step(arr, start, thread, debug->pass, depth);
				if(ret){
					sync_array_cell_print(buf, cell);
					printf("rw-lock %lx %s ", (ulint) lock, buf);
					rw_lock_debug_print(debug);

					return(TRUE);
				}
			}
			debug->UT_LIST_GET_NEXT(list, debug);
		}
		return FALSE;
	}
	else{ /*latch的类型不明*/
		ut_error;
	}

	return TRUE;
}

/*确定是否可以唤醒一个线程来获得锁*/
static ibool sync_arr_cell_can_wake_up(sync_cell_t* cell)
{
	mutex_t*	mutex;
	rw_lock_t*	lock;

	if(cell->request_type == SYNC_MUTEX){
		mutex = cell->wait_object;
		if(mutex_get_lock_word(mutex) == 0) /*锁是空闲的，可以获得锁*/
			return TRUE;
	}
	else if(cell->request_type == RW_LOCK_EX){
		lock = cell->wait_object;
		/*x-latch处理no locked状态,可以获得锁*/
		if(rw_lock_get_reader_count(lock) == 0 && rw_lock_get_writer(lock) == RW_LOCK_NOT_LOCKED){
			return TRUE;
		}

		/*x-latch处于wait状态，但是处于同一线程当中，可以获得锁*/
		if (rw_lock_get_reader_count(lock) == 0 && rw_lock_get_writer(lock) == RW_LOCK_WAIT_EX
			&& os_thread_eq(lock->writer_thread, cell->thread)) {
				return(TRUE);
		}
	}
	else if(cell->request_type == RW_LOCK_SHARED){ /*S-latch*/
		lock = cell->wait_object;
		/*处于no locked状态，可以获得锁*/
		if(rw_lock_get_writer(lock) == RW_LOCK_NOT_LOCKED)
			return TRUE;
	}

	return FALSE;
}
/*释放一个array cell单元,会自动释放sync_array_wait_event的信号，在这个cell重新使用的时候，会reset_event*/
static ibool sync_array_free_cell(sync_array_t* arr, ulint index)
{
	sync_cell_t* cell;
	sync_array_enter(arr);
	
	cell = sync_array_get_nth_cell(arr, index);
	ut_ad(cell->wait_object != NULL);

	cell->wait_object = NULL;
	ut_a(arr->n_reserved > 0);
	arr->n_reserved --;

	sync_array_exit(arr);
}

/*发送信号，让所有等待的cell全部放弃等待来获取各自的锁*/
void sync_array_signal_object(sync_array_t* arr, void* object)
{
	sync_cell_t*		cell;
	ulint				count;
	ulint				i;

	sync_array_enter(arr);
	arr->sg_count ++;

	i = 0;
	count = 0;
	while(count < arr->n_reserved){ /*遍历所有占用的cell,统一发送signal*/
		cell = sync_array_get_nth_cell(arr, i);
		if(cell->wait_object != NULL){
			if(cell->wait_object == object)
				sync_cell_event_set(cell);
		}
		i ++;
	}
	sync_array_exit(arr);
}

/*每秒必须调用一次这个函数，主要作用是释放所有可以获得锁的cell*/
void sync_arr_wake_threads_if_sema_free()
{
	sync_array_t*	arr = sync_primary_wait_array;
	sync_cell_t*	cell;
	ulint			count;
	ulint			i;

	sync_array_enter(arr);
	i = 0;
	count = 0;
	while(count < arr->n_reserved){
		cell = sync_array_get_nth_cell(arr, i);
		if(cell->wait_object != NULL){
			count ++;
			if(sync_arr_cell_can_wake_up(cell)) /*判断锁是否可以获得*/
				sync_cell_event_set(cell);
		}
		i ++;
	}

	sync_array_exit(arr);
}

void sync_array_print_long_waits()
{
	sync_cell_t*   	cell;
	ibool		old_val;
	ibool		noticed = FALSE;
	char		buf[500];
	ulint           i;

	for (i = 0; i < sync_primary_wait_array->n_cells; i++) {
		cell = sync_array_get_nth_cell(sync_primary_wait_array, i);
		if (cell->wait_object != NULL && difftime(time(NULL), cell->reservation_time) > 240) { /*cell占用的时间超过240秒，可以判断是一个长信号*/
				sync_array_cell_print(buf, cell);
				fprintf(stderr, "InnoDB: Warning: a long semaphore wait:\n%s", buf);
				noticed = TRUE;
		}

		if (cell->wait_object != NULL
			&& difftime(time(NULL), cell->reservation_time) > 600) { /*服务器可能僵死了*/
				fprintf(stderr, "InnoDB: Error: semaphore wait has lasted > 600 seconds\n"
					"InnoDB: We intentionally crash the server, because it appears to be hung.\n");
				ut_a(0);
		}
	}

	if (noticed) {
		fprintf(stderr,"InnoDB: ###### Starts InnoDB Monitor for 30 secs to print diagnostic info:\n");

		old_val = srv_print_innodb_monitor;
		srv_print_innodb_monitor = TRUE;
		os_event_set(srv_lock_timeout_thread_event);

		os_thread_sleep(30000000);

		srv_print_innodb_monitor = old_val;
		fprintf(stderr, "InnoDB: ###### Diagnostic info printed to the standard output\n");
	}
}

static void sync_array_output_info(char* buf, char*	buf_end, sync_array_t* arr)	
{
	sync_cell_t*   	cell;
	ulint           count;
	ulint           i;

	if (buf_end - buf < 500)
		return;

	buf += sprintf(buf,"OS WAIT ARRAY INFO: reservation count %ld, signal count %ld\n", arr->res_count, arr->sg_count);
	i = 0;
	count = 0;

	while (count < arr->n_reserved){
		if (buf_end - buf < 500) /*判断缓冲区长度*/
			return;

		cell = sync_array_get_nth_cell(arr, i);
		if(cell->wait_object != NULL){
			count++;
			sync_array_cell_print(buf, cell);
			buf = buf + strlen(buf);
		}
		i++;
	}
}

void sync_array_print_info(char* buf, char* buf_end, sync_array_t* arr)
{
	sync_array_enter(arr);
	sync_array_output_info(buf, buf_end, arr);
	sync_array_exit(arr);
}
















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

static void sync_array_cell_print(char* buf, sync_cell_t*	cell)
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
}






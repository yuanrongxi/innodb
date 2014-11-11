#ifdef __WIN__
#include <winbase.h>
#include <windows.h>
#endif

#include "os0sync.h"
#include "ut0mem.h"

struct os_mutex_struct
{
	void*		handle;
	ulint		count;
};

UNIV_INLINE ulint os_fast_mutex_trylock(os_fast_mutex_t* fast_mutex)
{
#ifdef __WIN__
	EnterCriticalSection(fast_mutex);
	return 0;
#else
	return((ulint) pthread_mutex_trylock(fast_mutex));
#endif
}

os_event_t os_event_create(char* name)
{
#ifdef __WIN__
	HANDLE	event;
	event = CreateEvent(NULL, TRUE, FALSE, name);
	if (!event)
		fprintf(stderr,"InnoDB: Could not create a Windows event semaphore; Windows error %lu\n", (ulint)GetLastError());
	ut_a(event);
	return(event);
#else
	os_event_t event;
	UT_NOT_USED(name);

	event = ut_malloc(sizeof(struct os_event_struct));
	os_fast_mutex_init(&event->os_mutex);
	event->is_set = FALSE;

	return event;
#endif
}

os_event_t os_event_create_auto(char* name)
{
#ifdef __WIN__
	HANDLE event;
	event = CreateEvent(NULL, FALSE, FALSE, name);
	ut_a(event);
	return event;
#else
	UT_NOT_USED(name);
	return NULL;
#endif
}

void os_event_set(os_event_t event)
{
#ifdef __WIN__
	ut_a(event);
	ut_a(SetEvent(event));
#else
	ut_a(event);
	os_fast_mutex_lock(event->os_mutex);
	if(!event->is_set){
		event->is_event = TRUE;
		pthread_cond_broadcast(&(event->cond_var));
	}
	os_fast_mutex_unlock(&(event->os_mutex));
#endif
}

void os_event_reset(os_event_t event)
{
#ifdef __WIN__
	ut_a(event);
	ut_a(ResetEvent(event));
#else
	ut_a(event);
	os_fast_mutex_lock(&(event_os_mutex));
	if(event->is_set)
		event->is_set = FALSE;
	os_fast_mutex_unlock(&(event->os_mutex));
#endif
}

void os_event_free(os_event_t event)
{
#ifndef __WIN__
	ut_a(event);
	ut_a(CloseHandle(event));
#else
	ut_a(event);
	os_fast_mutex_free(&(event->os_mutex));
#endif
}

void os_event_wait(os_event_t event)
{
#ifdef __WIN__
	DWORD	err;
	ut_a(event);
	/* Specify an infinite time limit for waiting */
	err = WaitForSingleObject(event, INFINITE);
	ut_a(err == WAIT_OBJECT_0);
#else /*多个线程同时抢is_set = TRUE的信号，在调用完这个函数后，需要调用os_event_reset来说设置从新等待*/
	os_fast_mutex_lock(&(event->os_mutex));
loop:
	if(event->is_set){
		os_fast_mutex_unlock(&(event->os_mutex));
		return;
	}

	pthread_cond_wait(&(event->cond_var), &(event->os_mutex));
	goto loop;
#endif
}

void os_event_wait_time(os_event_t event, ulint time)
{
#ifdef __WIN__
	DWORD	err;
	ut_a(event);
	if (time != OS_SYNC_INFINITE_TIME) {
		err = WaitForSingleObject(event, time / 1000);
	} else {
		err = WaitForSingleObject(event, INFINITE);
	}

	if (err == WAIT_OBJECT_0) {

		return(0);
	} else if (err == WAIT_TIMEOUT) {

		return(OS_SYNC_TIME_EXCEEDED);
	} else {
		ut_error;
		return(1000000); /* dummy value to eliminate compiler warn. */
	}
#else /*在posix模式下无法做到定时等待，只能无限等待直到信号触发*/
	os_event_wait(event);
	return 0;
#endif
}

ulint os_event_wait_multiple(ulint n, os_event_t* event_array)
{
#ifdef __WIN__
	DWORD	index;
	ut_a(event_array);
	ut_a(n > 0);
	index = WaitForMultipleObjects(n, event_array, FALSE, INFINITE); 
	ut_a(index >= WAIT_OBJECT_0);
	ut_a(index < WAIT_OBJECT_0 + n);

	return(index - WAIT_OBJECT_0);
#else
	os_event_wait(*event_array);
	return 0;
#endif
}

os_mutex_t os_mutex_create(char* name)
{
#ifdef __WIN__
	HANDLE		mutex;
	os_mutex_t	mutex_str;

	mutex = CreateMutex(NULL,	/* No security attributes */
		FALSE,		/* Initial state: no owner */
		name);
	ut_a(mutex);

	mutex_str = ut_malloc(sizeof(os_mutex_str_t));

	mutex_str->handle = mutex;
	mutex_str->count = 0;

	return(mutex_str);
#else
	os_fast_mutex_t* os_mutex;
	os_mutex_t	mutex_str;
	UT_NOT_USED(name);
	os_mutex = ut_malloc(sizeof(os_fast_mutex_t));
	os_fast_mutex_init(os_mutex);

	mutex_str = ut_malloc(sizeof(os_mutex_str_t));
	mutex_str->handle = os_mutex;
	mutex_str->count = 0;

	return mutex_str;
#endif
}

void os_mutex_enter(os_mutex_t mutex)
{
#ifdef __WIN__
	DWORD	err;
	ut_a(mutex);

	/* Specify infinite time limit for waiting */
	err = WaitForSingleObject(mutex->handle, INFINITE);
	ut_a(err == WAIT_OBJECT_0);

	(mutex->count)++;
	ut_a(mutex->count == 1);
#else
	os_fast_mutex_lock(mutex->handle);
	mutex->count ++;
	ut_a(mutex->count == 1);
#endif
}

void os_mutex_exit(os_mutex_t mutex)
{
#ifdef __WIN__
	ut_a(mutex);
	ut_a(mutex->count == 1);
	(mutex->count)--;
	ut_a(ReleaseMutex(mutex->handle));
#else
	ut_a(mutex);
	ut_a(mutex->count == 1);
	mutex->count --;
	os_fast_mutex_unlock(mutex->handle);
#endif
}

void os_mutex_free(os_mutex_t	mutex)
{
#ifdef __WIN__
	ut_a(mutex);
	ut_a(CloseHandle(mutex->handle));
	ut_free(mutex);
#else
	os_fast_mutex_free(mutex->handle);
	ut_free(mutex->handle);
	ut_free(mutex);
#endif
}

void os_fast_mutex_init(os_fast_mutex_t* fast_mutex)
{
#ifdef __WIN__
	ut_a(fast_mutex);
	InitializeCriticalSection((LPCRITICAL_SECTION) fast_mutex);
#else
	pthread_mutex_init(fast_mutex, MY_MUTEX_INIT_FAST);
#endif
}

void os_fast_mutex_lock(os_fast_mutex_t* fast_mutex)
{
#ifdef __WIN__
	EnterCriticalSection((LPCRITICAL_SECTION) fast_mutex);
#else
	pthread_mutex_lock(fast_mutex);
#endif
}

void os_fast_mutex_free(os_fast_mutex_t*	fast_mutex)	
{
#ifdef __WIN__
	ut_a(fast_mutex);
	DeleteCriticalSection((LPCRITICAL_SECTION) fast_mutex);
#else
	pthread_mutex_destroy(fast_mutex);
#endif
}









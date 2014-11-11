#include "os0thread.h"

#ifdef __WIN__
#include <windows.h>
#endif

#include "srv0srv.h"

ibool os_thread_eq(os_thread_id_t a, os_thread_id_t b)
{
#ifdef __WIN__
	return (a == b ? TRUE : FALSE);
#else
	return (pthread_equal(a, b) ? TRUE : FALSE);
#endif
}

ulint os_thread_pf(os_thread_id_t a)
{
	return (ulint)a;
}

/*获得当前thread id*/
os_thread_id_t os_thread_get_curr_id()
{
#ifdef __WIN__
	return(GetCurrentThreadId());
#else
	return(pthread_self());
#endif
}

os_thread_t os_thread_create(
#ifndef __WIN__
	os_posix_f_t start_f,
#else
	ulint (*start_f)(void*),
#endif
	void* arg, os_thread_id_t* thread_id)
{
#ifdef __WIN__
	os_thread_t	thread;
	ulint win_thread_id;

	/*建立一个windows线程*/
	thread = CreateThread(NULL,	0, (LPTHREAD_START_ROUTINE)start_f, arg, 0, &win_thread_id);
	if (srv_set_thread_priorities) { /*设置线程优先级*/
	        ut_a(SetThreadPriority(thread, srv_query_thread_priority));
	}
	*thread_id = win_thread_id;
	return(thread);
#else
	int ret;
	os_thread_t pthread;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	/*创建一个线程*/
	ret = pthread_create(&pthread, &attr, start_f, arg);
	if(ret != 0){
		fprintf(stderr, "InnoDB: Error: pthread_create returned %d\n", ret);
		exit(1);
	}

	pthread_attr_destroy(&attr);
	/*设置线程优先级*/
	if(srv_set_thread_priorities)
		my_pthread_setprio(pthread, srv_query_thread_priority);

	*thread_id = pthread;

	return pthread;
#endif
}

os_thread_t os_thread_get_curr()
{
#ifdef __WIN__
	return(GetCurrentThread());
#else
	return(pthread_self());
#endif
}

void os_thread_yield()
{
#if defined(__WIN__)
	Sleep(0);
#elif (defined(HAVE_SCHED_YIELD) && defined(HAVE_SCHED_H))
	sched_yield();
#elif defined(HAVE_PTHREAD_YIELD_ZERO_ARG)
	pthread_yield();
#elif defined(HAVE_PTHREAD_YIELD_ONE_ARG)
	pthread_yield(0);
#else
	os_thread_sleep(0);
#endif
}

void os_thread_sleep(ulint tm)
{
#ifdef __WIN__
	Sleep(tm / 1000);
#else /*用select来做sleep*/
	struct timeval	t;
	t.tv_sec = tm / 1000000;
	t.tv_usec = tm % 1000000;
	select(0, NULL, NULL, NULL, &t);
#endif
}

void os_thread_set_priority(os_thread_t	handle,	ulint pri)	
{
#ifdef __WIN__
	int	os_pri;

	if (pri == OS_THREAD_PRIORITY_BACKGROUND) {
		os_pri = THREAD_PRIORITY_BELOW_NORMAL;
	} else if (pri == OS_THREAD_PRIORITY_NORMAL) {
		os_pri = THREAD_PRIORITY_NORMAL;
	} else if (pri == OS_THREAD_PRIORITY_ABOVE_NORMAL) {
		os_pri = THREAD_PRIORITY_HIGHEST;
	} else {
		ut_error;
	}

	ut_a(SetThreadPriority(handle, os_pri));
#else
	UT_NOT_USED(handle);
	UT_NOT_USED(pri);
#endif
}

ulint os_thread_get_priority(os_thread_t handle)
{
#ifdef __WIN__
	int	os_pri;
	ulint	pri;

	os_pri = GetThreadPriority(handle);

	if (os_pri == THREAD_PRIORITY_BELOW_NORMAL) {
		pri = OS_THREAD_PRIORITY_BACKGROUND;
	} else if (os_pri == THREAD_PRIORITY_NORMAL) {
		pri = OS_THREAD_PRIORITY_NORMAL;
	} else if (os_pri == THREAD_PRIORITY_HIGHEST) {
		pri = OS_THREAD_PRIORITY_ABOVE_NORMAL;
	} else {
		ut_error;
	}

	return(pri);
#else
	return(0);
#endif
}

ulint os_thread_get_last_error()
{
#ifdef __WIN__
	return GetLastError();
#else
	return 0;
#endif
}








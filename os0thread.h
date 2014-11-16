#ifndef os0thread_h_
#define os0thread_h_

#include "univ.h"

#ifdef __WIN__
#define OS_THREAD_MAX_N 1000
#else
#define OS_THREAD_MAX_N 10000
#endif

#define OS_THREAD_PRIORITY_NONE				100
#define OS_THREAD_PRIORITY_BACKGROUND		1
#define OS_THREAD_PRIORITY_NORMAL			2
#define OS_THREAD_PRIORITY_ABOVE_NORMAL		3

#ifdef __WIN__
typedef void*		os_thread_t;
typedef ulint		os_thread_id_t;
#else
typedef pthread_t	os_thread_t;
typedef os_thread_t	os_thread_id_t;
#endif

typedef void* (*os_posix_f_t)(void*);

ibool			os_thread_eq(os_thread_id_t a, os_thread_id_t b);
ulint			os_thread_pf(os_thread_id_t a);

os_thread_t		os_thread_create(
#ifndef __WIN__
	os_posix_f_t start_f,
#else
	ulint (*start_f)(void*),
#endif
	void* arg, os_thread_id_t* thread_id);

void			os_thread_exit(ulint code);

os_thread_id_t	os_thread_get_curr_id();

os_thread_t		os_thread_get_curr();

void			os_thread_wait(os_thread_t thread);

void			os_thread_yield();

void			os_thread_sleep(ulint tm);

ulint			os_thread_get_prority(os_thread_t handle);

void			os_thread_set_priority(os_thread_t handle, ulint pri);

ulint			os_thread_get_last_error();

#endif

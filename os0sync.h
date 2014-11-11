#ifndef __OS0SYNC_H_
#define __OS0SYNC_H_

#include "univ.h"

#ifdef __WIN__/*WINDOWS系统*/
#define os_fast_mutex_t CRITICAL_SECTION
typedef void*	os_event_t;
#else /*posix系统*/
typedef pthread_mutex	os_fast_mutex_t;
struct os_event_struct
{
	os_fast_mutex_t	os_mutex;
	ibool			is_set;
	pthread_cond_t	cond_var;
};
typedef struct os_event_struct os_event_struct_t;
typedef os_event_struct_t* os_event_t;
#endif

typedef struct os_mutex_struct os_mutex_str_t;
typedef os_mutex_str_t*	os_mutex_t;


#define OS_SYNC_INFINITE_TIME	((ulint)-1)
#define OS_SYNC_TIME_EXCEEDED	1

/*signal event相关函数*/
os_event_t	os_event_create(char* name);
os_event_t	os_event_create_auto(char* name);
void		os_event_set(os_event_t event);
void		os_event_reset(os_event_t event);
void		os_event_free(os_event_t event);
void		os_event_wait(os_event_t event);
ulint		os_event_wait_time(os_event_t event, ulint time);
ulint		os_event_wait_multiple(ulint n, os_event_t* event_array);

/*mutex相关函数*/
os_mutex_t	os_mutex_create(char* name);
void		os_mutex_enter(os_mutex_t mutex);
void		os_mutex_exit(os_mutex_t mutex);
void		os_mutex_free(os_mutex_t mutex);

void		os_fast_mutex_init(os_fast_mutex_t* fast_mutex);
void		os_fast_mutex_free(os_fast_mutex_t* fast_mutex);
void		os_fast_mutex_trylock(os_fast_mutex_t* fast_mutex);
void		os_fast_mutex_unlock(os_fast_mutex_t* fast_mutex);
void		os_fast_mutex_lock(os_fast_mutex_t* fast_mutex);

#endif






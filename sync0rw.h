#ifndef __SYNC0RW_H_
#define __SYNC0RW_H_

#include "univ.h"
#include "ut0lst.h"
#include "sync0sync.h"
#include "os0sync.h"

#undef rw_lock_t

#define RW_S_LATCH	1
#define RW_S_LATCH	2
#define RW_NO_LATCH	3

#define	RW_LOCK_MAGIC_N	22643

typedef struct rw_lock_struct		rw_lock_t;
typedef struct rw_lock_debug_struct rw_lock_debug_t;

typedef UT_LIST_BASE_NODE_T(rw_lock_t)	rw_lock_list_t;

extern rw_lock_list_t	rw_lock_list;
extern mutex_t			rw_lock_list_mutex;

extern mutex_t			rw_lock_debug_mutex;
extern os_event_t		rw_lock_debug_event;
extern ibool			rw_lock_debug_waiters;

/*S-latch的统计数据*/
extern	ulint			rw_s_system_call_count;	/*系统调用次数*/
extern	ulint			rw_s_spin_wait_count;	/*自旋等待的次数*/
extern	ulint			rw_s_exit_count;
extern	ulint			rw_s_os_wait_count;

/*X-latch的统计数据*/
extern	ulint			rw_x_system_call_count;
extern	ulint			rw_x_spin_wait_count;
extern	ulint			rw_x_os_wait_count;
extern	ulint			rw_x_exit_count;

/*rw_lock的接口*/
#define rw_lock_create(L)			rw_lock_create_func((L), __FILE__, __LINE__)
#define rw_lock_s_lock(M)			rw_lock_s_lock_func((M), 0, __FILE__, __LINE__)
#define rw_lock_s_lock_gen(M, P)	rw_lock_s_lock_func((M), (P), __FILE__, __LINE__)
#define rw_lock_s_lock_nowait(M)	rw_lock_s_lock_func_nowait((M), __FILE__, __LINE__)

#ifdef UNIV_SYNC_DEBUG
#define rw_lock_s_unlock(L)			rw_lock_s_unlock_func(L, 0)
#else
#define rw_lock_s_unlock(L)			rw_lock_s_unlock_func(L)
#endif

#ifdef UNIV_SYNC_DEBUG
#define rw_lock_s_unlock_gen(L, P)  rw_lock_s_unlock_func(L, P)
#else
#define rw_lock_s_unlock_gen(L, P)  rw_lock_s_unlock_func(L)
#endif

#define rw_lock_x_lock(M)			rw_lock_x_lock_func((M), 0, __FILE__, __LINE__)
#define rw_lock_x_lock_gen(M, P)	rw_lock_x_lock_func((M), (P), __FILE__, __LINE__)
#define rw_lock_x_lock_nowait(M)	rw_lock_x_lock_func_nowait_func((M), __FILE__, __LINE__)

#ifdef UNIV_SYNC_DEBUG
#define rw_lock_x_unlock(L)			rw_lock_x_unlock_func(L, 0)
#else
#define rw_lock_x_unlock(L)			rw_lock_x_unlock_func(L)
#endif

#ifdef UNIV_SYNC_DEBUG
#define rw_lock_x_unlock_gen(L, P)  rw_lock_x_unlock_func(L, P)
#else
#define rw_lock_x_unlock_gen(L, P)  rw_lock_x_unlock_func(L)
#endif

void					rw_lock_create_func(rw_lock_t* lock, char* cfile_name, ulint cline);

void					rw_lock_free(rw_lock_t* lock);

ibool					rw_lock_validate(rw_lock_t* lock);

UNIV_INLINE void		rw_lock_s_lock_func(rw_lock_t* lock, ulint pass, char* file_name, ulint line);

UNIV_INLINE ibool		rw_lock_s_lock_func_nowait(rw_lock_t* lock, char* file_name, ulint line);

UNIV_INLINE ibool		rw_lock_x_lock_func_nowait(rw_lock_t* lock, char* file_name, ulint line);

UNIV_INLINE void		rw_lock_s_unlock_func(rw_lock_t* lock, 
#ifdef UNIV_SYNC_DEBUG
	ulint pass, 
#endif
);

void					rw_lock_x_lock_func(rw_lock_t* lock, ulint pass, char* file_name, ulint line);
UNIV_INLINE void		rw_lock_x_unlock_func(rw_lock_t* lock, 
#ifdef UNIV_SYNC_DEBUG
	ulint pass,
#endif
);

UNIV_INLINE void		rw_lock_s_lock_direct(rw_lock_t* lock, char* file_name, ulint line);

UNIV_INLINE void		rw_lock_x_lock_direct(rw_lock_t* lock, char* file_name, ulint line);

void					rw_lock_x_lock_move_ownership(rw_lock_t* lock);

UNIV_INLINE void		rw_lock_s_unlock_direct(rw_lock_t* lock);

UNIV_INLINE	void		rw_lock_x_unlock_direct(rw_lock_t* lock);

void					rw_lock_set_level(rw_lock_t* lock, ulint level);

UNIV_INLINE ulint		rw_lock_get_x_lock_count(rw_lock_t* lock);

UNIV_INLINE ulint		rw_lock_get_waiters(rw_lock_t* lock);

UNIV_INLINE ulint		rw_lock_get_writer(rw_lock_t* lock);

UNIV_INLINE ulint		rw_lock_get_reader_count(rw_lock_t* lock);

ibool					rw_lock_own(rw_lock_t* lock, ulint lock_type);

ibool					rw_lock_is_locked(rw_lock_t* lock, ulint lock_type);

/*打印接口与调试接口*/
ulint					rw_lock_n_locked();

void					rw_lock_list_print_inf();

void					rw_lock_debug_mutex_enter();

void					rw_lock_debug_mutex_exit();

void					rw_lock_debug_print(rw_lock_debug_t* info);

/*rw_lock_t的定义*/
struct rw_lock_struct
{
	ulint				reader_count;	/*获得S-LATCH的读者个数*/
	ulint				writer;			/*获得X-LATCH的状态，主要有RW_LOCK_EX、RW_LOCK_WAIT_EX、RW_LOCK_NOT_LOCKED*/
	os_thread_id_t		writer_thread;	/*获得X-LATCH的线程ID*/
	ulint				writer_count;	/*同一线程中X-latch lock次数*/
	
	mutex_t				mutex;
	ulint				pass;

	ulint				waiters;		/*有读或者写在等待获得latch*/
	ibool				writer_is_wait_ex;

	UT_LIST_NODE_T(rw_lock_t) list;
	UT_LIST_BASE_NODE_T(rw_lock_debug_t) debug_list;

	ulint				level;

	/*用于调试的信息*/
	char*				cfile_name; /*rw_lock创建时的文件*/
	ulint				cline;		/*rw_lock创建是的文件行位置*/

	char*				last_s_file_name; /**/
	char*				last_x_file_name;
	ulint				last_s_line;
	ulint				last_x_line;

	ulint				magic_n;	/*魔法字*/
};

struct rw_lock_debug_struct
{
	os_thread_id_t		thread_id;	/*获得latch的线程ID	*/
	ulint				pass;		/**/
	ulint				lock_type;	/*rw_lock类型，RW_LOCK_EX、RW_LOCK_SHARED、RW_LOCK_WAIT_EX*/

	char*				file_name; /*获得latch的文件*/
	char*				line;	   /*获得latch的文件行位置*/

	UT_LIST_NODE_T(rw_lock_debug_t) list;
};

#endif





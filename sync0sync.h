#ifndef __SYNC0SYNC_H_
#define __SYNC0SYNC_H_

#include "univ.h"
#include "sync0types.h"
#include "ut0lst.h"
#include "ut0mem.h"
#include "os0thread.h"
#include "os0sync.h"
#include "sync0arr.h"

#define SYNC_USER_TRX_LOCK			9999
#define SYNC_NO_ORDER_CHECK			3000	/* this can be used to suppress	latching order checking */
#define	SYNC_LEVEL_NONE				2000	/* default: level not defined */
#define	SYNC_FOREIGN_KEY_CHECK		1001
#define SYNC_DICT					1000
#define SYNC_DICT_AUTOINC_MUTEX		999
#define	SYNC_PURGE_IS_RUNNING		997
#define SYNC_DICT_HEADER			995
#define SYNC_IBUF_HEADER			914
#define SYNC_IBUF_PESS_INSERT_MUTEX 912
#define SYNC_IBUF_MUTEX				910	/* ibuf mutex is really below SYNC_FSP_PAGE: we assign value this high only to get the program to pass the debug checks */
/*-------------------------------*/
#define	SYNC_INDEX_TREE				900
#define SYNC_TREE_NODE_NEW			892
#define SYNC_TREE_NODE_FROM_HASH	891
#define SYNC_TREE_NODE				890
#define	SYNC_PURGE_SYS				810
#define	SYNC_PURGE_LATCH			800
#define	SYNC_TRX_UNDO				700
#define SYNC_RSEG					600
#define SYNC_RSEG_HEADER_NEW		591
#define SYNC_RSEG_HEADER			590
#define SYNC_TRX_UNDO_PAGE			570
#define SYNC_EXTERN_STORAGE			500
#define	SYNC_FSP					400
#define	SYNC_FSP_PAGE				395
/*------------------------------------- Insert buffer headers */ 
/*------------------------------------- ibuf_mutex */
/*------------------------------------- Insert buffer trees */
#define	SYNC_IBUF_BITMAP_MUTEX		351
#define	SYNC_IBUF_BITMAP			350
/*-------------------------------*/
#define	SYNC_KERNEL					300
#define SYNC_REC_LOCK				299
#define	SYNC_TRX_LOCK_HEAP			298
#define SYNC_TRX_SYS_HEADER			290
#define SYNC_LOG					170
#define SYNC_RECV					168
#define	SYNC_SEARCH_SYS				160	/* NOTE that if we have a memory heap that can be extended to the buffer pool, its logical level is SYNC_SEARCH_SYS, as memory allocation can call routines there! Otherwise the level is SYNC_MEM_HASH. */
#define	SYNC_BUF_POOL				150
#define	SYNC_BUF_BLOCK				149
#define SYNC_DOUBLEWRITE			140
#define	SYNC_ANY_LATCH				135
#define SYNC_THR_LOCAL				133
#define	SYNC_MEM_HASH				131
#define	SYNC_MEM_POOL				130

/* Codes used to designate lock operations */
#define RW_LOCK_NOT_LOCKED 			350
#define RW_LOCK_EX					351
#define RW_LOCK_EXCLUSIVE			351
#define RW_LOCK_SHARED				352
#define RW_LOCK_WAIT_EX				353
#define SYNC_MUTEX					354

#define mutex_create(M)			mutex_create_func((M), __FILE__, __LINE__)
#define mutex_enter(M)			mutex_enter_func((M), __FILE__, __LINE__)
#define mutex_enter_fast(M)    	mutex_enter_func((M), __FILE__, __LINE__)
#define mutex_enter_fast_func  	mutex_enter_func;
/*****************************************************************/
void				sync_init();
void				sync_close();
void				mutex_create_func(mutex_t* mutex, char* file_name, ulint line);
#undef mutex_free
void				mutex_free(mutex_t* mutex);
UNIV_INLINE void	mutex_enter_func(mutex_t* mutex, char* file_name, ulint line);
ulint				mutex_enter_nowait(mutex_t* mutex, char* file_name, ulint line);
UNIV_INLINE void	mutex_exit(mutex_t* mutex);
ibool				sync_all_freed();

void				sync_print_wait_info(char* buf, char* buf_end);
void				sync_print(char* buf, char* buf_end);

ibool				mutex_validate(mutex_t* mutex);
void				mutex_set_level(mutex_t* mutex, ulint level);
void				sync_thread_add_level(void* latch, ulint level);
void				sync_thread_reset_level(void* latch);
ibool				sync_thread_levels_empty();
ibool				sync_thread_levels_empty_gen(ibool dict_mutex_allowed);

ibool				mutex_own(mutex_t* mutex);
void				mutex_get_debug_info(mutex_t* mutex, char** file_name, ulint* line, os_thread_id_t* thread_id);
ulint				mutex_n_reserved();
void				mutex_list_print_info();

UNIV_INLINE ulint	mutex_get_lock_word(mutex_t* mutex);
UNIV_INLINE ulint	mutex_get_waiters(mutex_t* mutex);

void				mutex_fence();


#define MUTEX_MAGIC_N	(ulint)979585

extern sync_array_t*	sync_primary_wait_array;

/*自旋锁自选的周期数*/
#define SYNC_SPIN_ROUNDS srv_n_spin_wait_rounds
/*无限自旋*/
#define SYNC_INFINITE_TIME ((ulint)-1)

#define SYNC_TIME_EXCEEDED	(ulint)1

extern ulint mutex_system_call_count;
extern ulint mutex_exit_count;

extern ibool sync_order_checks_on;
extern ibool sync_initialized;

struct mutex_struct
{
	ulint					lock_word;		/*mutex原子控制变量*/
	os_fast_mutex_t			os_fast_mutex;	/*在编译器或者系统部支持原子操作的时候采用的系统os_mutex来替代mutex*/
	ulint					waiters;		/*是否有线程在等待锁*/
	UT_LIST_NODE_T(mutex_t)	list;			/*mutex list node*/
	os_thread_id_t			thread_id;		/*获得mutex的线程ID*/
	
	char*					file_name;		/*mutex lock的位置*/
	ulint					line;

	ulint					level;			/*锁层ID*/

	char*					cfile_name;		/*mute创建的位置*/
	ulint					cline;		
	
	ulint					magic_n;		/*魔法字*/
};

#endif
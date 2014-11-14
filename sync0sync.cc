#include "sync0sync.h"
#include "sync0rw.h"
#include "buf0buf.h"
#include "sev0srv.h"
#include "buf0types.h"

ulint	sync_dummy				= 0;
ulint	mutex_system_call_count = 0;
ulint	mutex_spin_wait_count	= 0;
ulint	mutex_os_wait_count		= 0;
ulint	mutex_exit_count		= 0;
ibool	sync_initialized		= FALSE;
ibool	sync_order_checks_on	= FALSE;

mutex_t	mutex_list_mutex;
mutex_t dummy_mutex_for_fence;
mutex_t sync_thread_mutex;

UT_LIST_BASE_NODE_T(mutex_t)	mutex_list;

typedef struct sync_level_struct sync_level_t;
typedef struct sync_thread_struct sync_thread_t;

sync_thread_t*	sync_thread_level_arrays;

struct sync_thread_struct
{
	os_thread_id_t	id;
	sync_level_t*	levels;
};

struct sync_level_struct
{
	void*	latch;
	ulint	level;
};

#define SYNC_THREAD_N_LEVELS	10000

void mutex_set_waiters(mutex_t* mutex, ulint n);
void mutex_spin_wait(mutex_t* mutex, char* file_name, ulint line);
void mutex_set_debug_info(mutex_t* mutex, char* file_name, ulint line);
void mutex_signal_object(mutex_t* mutex);

UNIV_INLINE ulint mutex_test_and_set(mutex_t* mutex)	/* in: mutex */
{
#if defined(_WIN32) && defined(UNIV_CAN_USE_X86_ASSEMBLER)
	ulint	res;
	ulint*	lw;		/* assembler code is used to ensure that
				lock_word is loaded from memory */
	ut_ad(mutex);
	ut_ad(sizeof(ulint) == 4);

	lw = &(mutex->lock_word);

        __asm   MOV     ECX, lw
		__asm   MOV     EDX, 1
        __asm   XCHG    EDX, DWORD PTR [ECX]                    
       	__asm   MOV     res, EDX

	/* The fence below would prevent this thread from reading the data
	structure protected by the mutex before the test-and-set operation is
	committed, but the fence is apparently not needed:

	In a posting to comp.arch newsgroup (August 10, 1997) Andy Glew said
	that in P6 a LOCKed instruction like XCHG establishes a fence with
	respect to memory reads and writes and thus an explicit fence is not
	needed. In P5 he seemed to agree with a previous newsgroup poster that
	LOCKed instructions serialize all instruction execution, and,
	consequently, also memory operations. This is confirmed in Intel
	Software Dev. Manual, Vol. 3. */

	/* mutex_fence(); */

	return(res);
#elif defined(not_defined) && defined(__GNUC__) && defined(UNIV_INTEL_X86) /*原子操作*/
	ulint*	lw;
	ulint	res;

	lw = &(mutex->lock_word);

	/* In assembly we use the so-called AT & T syntax where
	the order of operands is inverted compared to the ordinary Intel
	syntax. The 'l' after the mnemonics denotes a 32-bit operation.
	The line after the code tells which values come out of the asm
	code, and the second line tells the input to the asm code. */

	asm volatile("movl $1, %%eax; xchgl (%%ecx), %%eax" :
	              "=eax" (res), "=m" (*lw) :
	              "ecx" (lw));
	return(res);
#else
	ibool ret = os_fast_mutex_trylock(&(mutex->os_fast_mutex));
	if(ret == 0) {
		/* We check that os_fast_mutex_trylock does not leak
		and allow race conditions */
		ut_a(mutex->lock_word == 0);

		mutex->lock_word = 1;
	}

	return(ret);
#endif
}

UNIV_INLINE void mutex_reset_lock_word(mutex_t*	mutex)	/* in: mutex */
{
#if defined(_WIN32) && defined(UNIV_CAN_USE_X86_ASSEMBLER)
	ulint*	lw;		/* assembler code is used to ensure that
				lock_word is loaded from memory */
	ut_ad(mutex);

	lw = &(mutex->lock_word);

	__asm   MOV     EDX, 0
        __asm   MOV     ECX, lw
        __asm   XCHG    EDX, DWORD PTR [ECX]                    
#elif defined(not_defined) && defined(__GNUC__) && defined(UNIV_INTEL_X86)
	ulint*	lw;

	lw = &(mutex->lock_word);

	/* In assembly we use the so-called AT & T syntax where
	the order of operands is inverted compared to the ordinary Intel
	syntax. The 'l' after the mnemonics denotes a 32-bit operation. */

	asm volatile("movl $0, %%eax; xchgl (%%ecx), %%eax" :
	              "=m" (*lw) :
	              "ecx" (lw) :
		      "eax");	/* gcc does not seem to understand
				that our asm code resets eax: tell it
				explicitly that after the third ':' */
#else
	mutex->lock_word = 0;
	os_fast_mutex_unlock(&(mutex->os_fast_mutex));
#endif
}

UNIV_INLINE ulint mutex_get_lock_word(mutex_t*	mutex)
{
	volatile ulint*	ptr;		/* declared volatile to ensure that lock_word is loaded from memory */
	ut_ad(mutex);
	ptr = &(mutex->lock_word);
	return *ptr;
}

UNIV_INLINE ulint mutex_get_waiters(mutex_t* mutex)
{
	volatile ulint*	ptr;
	ut_ad(mutex);
	ptr = &(mutex->waiters);
	return *ptr;
}

UNIV_INLINE void mutex_exit(mutex_t* mutex)
{
	ut_ad(mutex_own(mutex));
	mutex_reset_lock_word(mutex);

	/*锁被释放*/
	if (mutex_get_waiters(mutex) != 0)
		mutex_signal_object(mutex);

	mutex_exit_count ++;
}

UNIV_INLINE void mutex_enter_func(mutex_t* mutex, char* file_name, ulint line)
{
	ut_ad(mutex_validate(mutex));
	if(!mutex_test_and_set(mutex)){ /*如果获得锁，修改锁的位置*/
		mutex->file_name = file_name;
		mutex->line = line;
	}
	else /*没有获得锁，自旋等待*/
		mutex_spin_wait(mutex, file_name, line);
}

/*相当于现在的CPU atomic操作*/
#if defined(notdefined) && defined(__GNUC__) && defined(UNIV_INTEL_X86)
ulint sync_gnuc_intelx86_test_and_set(ulint* lw) /* in: pointer to the lock word */
{
        ulint res;

	/* In assembly we use the so-called AT & T syntax where
	the order of operands is inverted compared to the ordinary Intel
	syntax. The 'l' after the mnemonics denotes a 32-bit operation.
	The line after the code tells which values come out of the asm
	code, and the second line tells the input to the asm code. */

	asm volatile("movl $1, %%eax; xchgl (%%ecx), %%eax" :
	              "=eax" (res), "=m" (*lw) :
	              "ecx" (lw));
	return(res);
}

void sync_gnuc_intelx86_reset(ulint* lw) /* in: pointer to the lock word */
{
	/* In assembly we use the so-called AT & T syntax where
	the order of operands is inverted compared to the ordinary Intel
	syntax. The 'l' after the mnemonics denotes a 32-bit operation. */

	asm volatile("movl $0, %%eax; xchgl (%%ecx), %%eax" :
	              "=m" (*lw) :
	              "ecx" (lw) :
		      "eax");	/* gcc does not seem to understand
				that our asm code resets eax: tell it
				explicitly that after the third ':' */
}

#endif

void mutex_create_func(mutex_t* mutex, char* cfile_name, ulint cline)
{
#if defined(_WIN32) && defined(UNIV_CAN_USE_X86_ASSEMBLER) 
	mutex_reset_lock_word(mutex);
#else
	os_fast_mutex_init(&(mutex->os_fast_mutex));
	mutex->lock_word = 0;
#endif

	mutex_set_waiters(mutex, 0);
	mutex->magic_n = MUTEX_MAGIC_N;
	mutex->line = 0;
	mutex->file_name = "no yet reserved";
	mutex->level = SYNC_LEVEL_NONE;
	mutex->cfile_name = cfile_name;
	mutex->cline = cline;

	ut_a(((ulint)(&mutex->lock_word))%4 == 0);
	if(mutex == &mutex_list_mutex || mutex == &sync_thread_mutex)
		return ;

	mutex_enter(&mutex_list_mutex);
	UT_LIST_ADD_FIRST(list, mutex_list, mutex);
	mutex_exit(&mutex_list_mutex);
}


void mutex_free(mutex_t* mutex)
{
	ut_ad(mutex_validate(mutex));
	ut_a(mutex_get_lock_word(mutex) == 0);
	ut_a(mutex_get_waiters(mutex) == 0);

	mutex_enter(&mutex_list_mutex);
	UT_LIST_REMOVE(list, mutex_list, mutex);
	mutex_exit(&mutex_list_mutex);

#if !defined(_WIN32) || !defined(UNIV_CAN_USE_X86_ASSEMBLER) 
	os_fast_mutex_free(&(mutex->os_fast_mutex));
#endif

	mutex->magic_n = 0;
}

ulint mutex_enter_nowait(mutex_t*	mutex, char* file_name, ulint line)
{
	ut_ad(mutex_validate(mutex));
	if (!mutex_test_and_set(mutex)){
#ifdef UNIV_SYNC_DEBUG
			mutex_set_debug_info(mutex, file_name, line);
#endif
		
		mutex->file_name = file_name;
		mutex->line = line;

		return(0);
	}
	return(1);
}

/*魔法字校验，仅仅在DEBUG下使用*/
ibool mutex_validate(mutex_t* mutex)
{
	ut_a(mutex);
	ut_a(mutex->mutex_magic_n == MUTEX_MAGIC_N);
	return TRUE;
}

void mutex_set_waiters(mutex_t* mutex, ulint n)
{
	volatile ulint*	ptr;

	ut_ad(mutex);
	ptr = &(mutex->waiters);
	*ptr = n;
}

void mutex_spin_wait(mutex_t* mutex, char* file_name, ulint line)
{
	ulint index;
	ulint i;

	ut_ad(mutex);

mutex_loop:
	i = 0;

spin_loop:
	mutex_spin_wait_count ++; /*增加自选次数*/
	while(mutex_get_lock_word(mutex) != 0 && i < SYNC_SPIN_ROUNDS){ /*锁已经可以被获得或者自旋到了一个ROUNDS将退出循环*/
		if(srv_spin_wait_delay) /*如果MySQL设置了等待的话，进行sleep等待*/
			ut_delay(ut_rnd_interval(0, srv_spin_wait_delay));

		i ++;
	}

	if(i == SYNC_SPIN_ROUNDS)
		os_thread_yield();

	if(srv_print_latch_waits){ /*状态信息打印*/
		printf("Thread %lu spin wait mutex at %lx cfile %s cline %lu rnds %lu\n", os_thread_pf(os_thread_get_curr_id()), 
			(ulint)mutex, mutex->cfile_name, mutex->cline, i);
	}

	/*增加自旋的计数器*/
	mutex_spin_wait_count += i;

	if(mutex_test_and_set(mutex) == 0){ /*获得锁成功*/
		mutex->file_name = file_name;
		mutex->line = line;
		return;
	}

	i ++;
	if(i < SYNC_SPIN_ROUNDS) /*回到自旋的位子继续自旋等待*/
		goto spin_loop;

	/*sync_array_reserve_cell算一次系统调用*/
	mutex_system_call_count++;

	/*加入到array cell中*/
	sync_array_reserve_cell(sync_primary_wait_array, mutex, SYNC_MUTEX, file_name, line, &index);

	mutex_set_waiters(mutex, 1);
	
	for(i = 0; i < 4; i ++){
		/*尝试获得锁*/
		if(mutex_test_and_set(mutex) == 0){ /*获得锁，释放掉array cell的状态*/
			sync_array_free_cell(sync_primary_wait_array, index);

			mutex->file_name = file_name;
			mutex->line = line;

			if(srv_print_latch_waits)
				printf("Thread %lu spin wait succeeds at 2: mutex at %lx\n", os_thread_pf(os_thread_get_curr_id()), (ulint)mutex);

			return;
		}
	}

	if(srv_print_latch_waits){
		printf("Thread %lu OS wait mutex at %lx cfile %s cline %lu rnds %lu\n",
			os_thread_pf(os_thread_get_curr_id()), (ulint)mutex,
			mutex->cfile_name, mutex->cline, i);
	}

	/*增加一次系统调用和操作系统作业调度等待*/
	mutex_system_call_count ++;
	mutex_os_wait_count ++;

	sync_array_wait_event(sync_primary_wait_array, index);

	/*重新尝试获得锁*/
	goto mutex_loop;
}

void mutex_signal_object(mutex_t* mutex)
{
	mutex_set_waiters(mutex, 0);
	/*通过cell array触发一个信号让waiters获得锁*/
	sync_array_signal_object(sync_primary_wait_array, mutex);
}

void mutex_set_debug_info(mutex_t* mutex, char* file_name, ulint line)
{
	ut_ad(mutex);
	ut_ad(file_name);

	sync_thread_add_level(mutex, mutex->level);

	mutex->file_name = file_name;
	mutex->line = line;
	mutex->thread_id = os_thread_get_curr_id();
}

void mutex_set_level(mutex_t* mutex, ulint level)
{
	mutex->level = level;
}

ibool mutex_own(mutex_t* mutex)
{
	ut_a(mutex_validate(mutex));
	if(mutex_get_lock_word(mutex) != -1) /*锁没有被获取*/
		return FALSE;

	/*判断是否是本线程获得锁*/
	if(!os_thread_eq(mutex->thread_id, os_thread_get_curr_id()))
		return FALSE;
	else
		return TRUE;
}

void mutex_list_print_info(void)
{
#ifndef UNIV_SYNC_DEBUG
#else
	mutex_t*	mutex;
	char*		file_name;
	ulint		line;
	os_thread_id_t	thread_id;
	ulint		count		= 0;

	printf("----------\n");
	printf("MUTEX INFO\n");
	printf("----------\n");

	mutex_enter(&mutex_list_mutex);

	mutex = UT_LIST_GET_FIRST(mutex_list);

	while (mutex != NULL) {
		count++;

		if (mutex_get_lock_word(mutex) != 0) {
			mutex_get_debug_info(mutex, &file_name, &line,
				&thread_id);
			printf(
				"Locked mutex: addr %lx thread %ld file %s line %ld\n",
				(ulint)mutex, os_thread_pf(thread_id),
				file_name, line);
		}

		mutex = UT_LIST_GET_NEXT(list, mutex);
	}

	printf("Total number of mutexes %ld\n", count);

	mutex_exit(&mutex_list_mutex);
#endif
}

ulint mutex_n_reserved()
{
#ifndef UNIV_SYNC_DEBUG
	printf("Sorry, cannot give mutex info in non-debug version!\n");
	ut_error;

	return(0);
#else /*判断mutex_list中有多少个mutex是空闲的*/
	mutex_t*	mutex;
	ulint		count = 0;

	mutex_enter(&mutex_list_mutex);
	mutex = UT_LIST_GET_FIRST(mutex_list);
	while (mutex != NULL) {
		if (mutex_get_lock_word(mutex) != 0)
			count++;

		mutex = UT_LIST_GET_NEXT(list, mutex);
	}

	mutex_exit(&mutex_list_mutex);
	ut_a(count >= 1);
	return(count - 1);
#endif
}

/*判断所有锁是不是都是空闲的*/
ibool sync_all_freed()
{
#ifdef UNIV_SYNC_DEBUG
	if (mutex_n_reserved() + rw_lock_n_locked() == 0)
		return(TRUE);
	else 
		return(FALSE);
#else
	ut_error;
	return(FALSE);
#endif
}

static sync_thread_t* sync_thread_level_arrays_get_nth(ulint n)
{
	ut_ad(n < OS_THREAD_MAX_N);
	return sync_thread_level_arrays + n;
}

/*查找本线程的sync_thread_t,只有有latch才会有*/
static sync_thread_t* sync_thread_level_arrays_find_slot()
{
	sync_thread_t* slot;
	os_thread_id_t id;
	ulint i;

	id = os_thread_get_curr_id();
	for(i = 0; i < OS_THREAD_MAX_N; i++){
		slot = sync_thread_level_arrays_get_nth(i);
		if(slot->levels && os_thread_eq(slot->id, id))
			return(slot);
	}

	return NULL;
}

static sync_thread_t* sync_thread_level_arrays_find_free()
{
	sync_thread_t* slot;
	ulint i;
	for(i = 0; i < OS_THREAD_MAX_N; i ++){
		slot = sync_thread_level_arrays_get_nth(i);
		if(slot->levels == NULL) /*levels没有使用*/
			return slot;
	}

	return NULL;
}

static sync_level_t* sync_thread_levels_get_nth(sync_level_t* arr, ulint n)
{
	ut_ad(n < SYNC_THREAD_N_LEVELS);
	return arr + n;
}

static ibool sync_thread_levels_g(sync_level_t* arr, ulint limit)
{
	char*			file_name;
	ulint			line;
	os_thread_id_t	thread_id;
	sync_level_t*	slot;
	rw_lock_t*		lock;
	mutex*			mutex;
	ulint			i;

	for(i = 0; i < SYNC_THREAD_N_LEVELS; i ++){
		slot = sync_thread_levels_get_nth(arr, i);
		if(slot->latch != NULL){
			if(slot->level <= limit){
				lock = slot->latch;  /*rw_lock方式*/
				mutex = slot->latch; /*mutex方式*/

				printf("InnoDB error: sync levels should be > %lu but a level is %lu\n", limit, slot->level);
				if(mutex->magic_n == MUTEX_MAGIC_N){ /*如果魔法字和mutex的魔法字匹配时，说明latch是一个mutex*/
					printf("Mutex created at %s %lu\n", mutex->cfile_name, mutex->cline);

					if (mutex_get_lock_word(mutex) != 0) { /*锁是空闲的*/
						mutex_get_debug_info(mutex,&file_name, &line, &thread_id);
						printf("InnoDB: Locked mutex: addr %lx thread %ld file %s line %ld\n", (ulint)mutex, os_thread_pf(thread_id),file_name, line);
					}
					else
						printf("Not locked\n");
				}
				else{
					rw_lock_print(lock);
				}

				return FALSE;
			}
		}
	}

	return TRUE;
}
/*thread levels当中是否包含level的slot latch*/
static ibool sync_thread_levels_contain(sync_level_t* arr, ulint level)
{
	sync_level_t* slot;
	ulint i;
	for(i = 0; i < SYNC_THREAD_N_LEVELS; i ++){
		slot = sync_thread_levels_get_nth(arr, i);
		if(slot->latch != NULL){
			if(slot->level == level)
				return TRUE;
		}
	}

	return FALSE;
}

ibool sync_thread_levels_empty_gen(ibool dict_mutex_allowed)
{
	sync_level_t*	arr;
	sync_thread_t*	thread_slot;
	sync_level_t*	slot;
	rw_lock_t*		lock;
	mutex_t*		mutex;
	char*			buf;
	ulint			i;

	if(!sync_order_checks_on)
		return TRUE;

	mutex_enter(&sync_thread_mutex);
	thread_slot = sync_thread_level_arrays_find_slot();
	if(thread_slot == NULL){ /*本线程无latch slot*/
		mutex_exit(&sync_thread_mutex);
		return TRUE;
	}

	arr = thread_slot->levels;
	for(i = 0; i < SYNC_THREAD_N_LEVELS; i++){ 
		slot = sync_thread_levels_get_nth(arr, i);
		if(slot->latch != NULL && (!dict_mutex_allowed ||(slot->level != SYNC_DICT && slot->level != SYNC_FOREIGN_KEY_CHECK && slot->level != SYNC_PURGE_IS_RUNNING)))
		{
			lock = slot->latch;
			mutex = slot->latch;
			mutex_exit(&sync_thread_mutex);

			buf = mem_alloc(20000);

			sync_print(buf, buf + 18000);
			ut_error;

			return(FALSE);
		}
	}

	mutex_exit(&sync_thread_mutex);

	return TRUE;
}

ibool sync_thread_levels_empty()
{
	return sync_thread_levels_empty_gen(FALSE);
}

void sync_thread_add_level(void* latch, ulint level)
{
	sync_level_t*	array;
	sync_level_t*	slot;
	sync_thread_t*	thread_slot;
	ulint			i;

	if(!sync_order_checks_on)
		return ;

	/*不是全局锁*/
	if ((latch == (void*)&sync_thread_mutex) || (latch == (void*)&mutex_list_mutex)|| (latch == (void*)&rw_lock_debug_mutex) || (latch == (void*)&rw_lock_list_mutex))
		return;

	if(level == SYNC_LEVEL_NONE)
		return ;

	mutex_enter(&sync_thread_mutex);
	/*找本线程的latch slot*/
	thread_slot = sync_thread_level_arrays_find_slot();
	if(thread_slot == NULL){
		/*创建一个sync_level_t数组*/
		array = ut_malloc(sizeof(sync_level_t) * SYNC_THREAD_N_LEVELS);
		thread_slot = sync_thread_level_arrays_find_free();
		thread_slot->id = os_thread_get_curr_id();
		thread_slot->levels = array;

		/*设置各层的level值*/
		for (i = 0; i < SYNC_THREAD_N_LEVELS; i++){
			slot = sync_thread_levels_get_nth(array, i);
			slot->latch = NULL;
		}
	}

	array = thread_slot->levels;
	if (level == SYNC_NO_ORDER_CHECK) {
		/* Do no order checking */

	} else if (level == SYNC_MEM_POOL) {
		ut_a(sync_thread_levels_g(array, SYNC_MEM_POOL));
	} else if (level == SYNC_MEM_HASH) {
		ut_a(sync_thread_levels_g(array, SYNC_MEM_HASH));
	} else if (level == SYNC_RECV) {
		ut_a(sync_thread_levels_g(array, SYNC_RECV));
	} else if (level == SYNC_LOG) {
		ut_a(sync_thread_levels_g(array, SYNC_LOG));
	} else if (level == SYNC_THR_LOCAL) {
		ut_a(sync_thread_levels_g(array, SYNC_THR_LOCAL));
	} else if (level == SYNC_ANY_LATCH) {
		ut_a(sync_thread_levels_g(array, SYNC_ANY_LATCH));
	} else if (level == SYNC_TRX_SYS_HEADER) {
		ut_a(sync_thread_levels_g(array, SYNC_TRX_SYS_HEADER));
	} else if (level == SYNC_DOUBLEWRITE) {
		ut_a(sync_thread_levels_g(array, SYNC_DOUBLEWRITE));
	} else if (level == SYNC_BUF_BLOCK) {
		ut_a((sync_thread_levels_contain(array, SYNC_BUF_POOL)
			&& sync_thread_levels_g(array, SYNC_BUF_BLOCK - 1))
			|| sync_thread_levels_g(array, SYNC_BUF_BLOCK));
	} else if (level == SYNC_BUF_POOL) {
		ut_a(sync_thread_levels_g(array, SYNC_BUF_POOL));
	} else if (level == SYNC_SEARCH_SYS) {
		ut_a(sync_thread_levels_g(array, SYNC_SEARCH_SYS));
	} else if (level == SYNC_TRX_LOCK_HEAP) {
		ut_a(sync_thread_levels_g(array, SYNC_TRX_LOCK_HEAP));
	} else if (level == SYNC_REC_LOCK) {
		ut_a((sync_thread_levels_contain(array, SYNC_KERNEL)
			&& sync_thread_levels_g(array, SYNC_REC_LOCK - 1))
			|| sync_thread_levels_g(array, SYNC_REC_LOCK));
	} else if (level == SYNC_KERNEL) {
		ut_a(sync_thread_levels_g(array, SYNC_KERNEL));
	} else if (level == SYNC_IBUF_BITMAP) {
		ut_a((sync_thread_levels_contain(array, SYNC_IBUF_BITMAP_MUTEX)
			&& sync_thread_levels_g(array, SYNC_IBUF_BITMAP - 1))
			|| sync_thread_levels_g(array, SYNC_IBUF_BITMAP));
	} else if (level == SYNC_IBUF_BITMAP_MUTEX) {
		ut_a(sync_thread_levels_g(array, SYNC_IBUF_BITMAP_MUTEX));
	} else if (level == SYNC_FSP_PAGE) {
		ut_a(sync_thread_levels_contain(array, SYNC_FSP));
	} else if (level == SYNC_FSP) {
		ut_a(sync_thread_levels_contain(array, SYNC_FSP)
			|| sync_thread_levels_g(array, SYNC_FSP));
	} else if (level == SYNC_EXTERN_STORAGE) {
		ut_a(TRUE);
	} else if (level == SYNC_TRX_UNDO_PAGE) {
		ut_a(sync_thread_levels_contain(array, SYNC_TRX_UNDO)
			|| sync_thread_levels_contain(array, SYNC_RSEG)
			|| sync_thread_levels_contain(array, SYNC_PURGE_SYS)
			|| sync_thread_levels_g(array, SYNC_TRX_UNDO_PAGE));
	} else if (level == SYNC_RSEG_HEADER) {
		ut_a(sync_thread_levels_contain(array, SYNC_RSEG));
	} else if (level == SYNC_RSEG_HEADER_NEW) {
		ut_a(sync_thread_levels_contain(array, SYNC_KERNEL)
			&& sync_thread_levels_contain(array, SYNC_FSP_PAGE));
	} else if (level == SYNC_RSEG) {
		ut_a(sync_thread_levels_g(array, SYNC_RSEG));
	} else if (level == SYNC_TRX_UNDO) {
		ut_a(sync_thread_levels_g(array, SYNC_TRX_UNDO));
	} else if (level == SYNC_PURGE_LATCH) {
		ut_a(sync_thread_levels_g(array, SYNC_PURGE_LATCH));
	} else if (level == SYNC_PURGE_SYS) {
		ut_a(sync_thread_levels_g(array, SYNC_PURGE_SYS));
	} else if (level == SYNC_TREE_NODE) {
		ut_a(sync_thread_levels_contain(array, SYNC_INDEX_TREE)
			|| sync_thread_levels_g(array, SYNC_TREE_NODE - 1));
	} else if (level == SYNC_TREE_NODE_FROM_HASH) {
		ut_a(1);
	} else if (level == SYNC_TREE_NODE_NEW) {
		ut_a(sync_thread_levels_contain(array, SYNC_FSP_PAGE)
			|| sync_thread_levels_contain(array, SYNC_IBUF_MUTEX));
	} else if (level == SYNC_INDEX_TREE) {
		ut_a((sync_thread_levels_contain(array, SYNC_IBUF_MUTEX)
			&& sync_thread_levels_contain(array, SYNC_FSP)
			&& sync_thread_levels_g(array, SYNC_FSP_PAGE - 1))
			|| sync_thread_levels_g(array, SYNC_TREE_NODE - 1));
	} else if (level == SYNC_IBUF_MUTEX) {
		ut_a(sync_thread_levels_g(array, SYNC_FSP_PAGE - 1));
	} else if (level == SYNC_IBUF_PESS_INSERT_MUTEX) {
		ut_a(sync_thread_levels_g(array, SYNC_FSP - 1)
			&& !sync_thread_levels_contain(array, SYNC_IBUF_MUTEX));
	} else if (level == SYNC_IBUF_HEADER) {
		ut_a(sync_thread_levels_g(array, SYNC_FSP - 1)
			&& !sync_thread_levels_contain(array, SYNC_IBUF_MUTEX)
			&& !sync_thread_levels_contain(array,
			SYNC_IBUF_PESS_INSERT_MUTEX));
	} else if (level == SYNC_DICT_AUTOINC_MUTEX) {
		ut_a(sync_thread_levels_g(array, SYNC_DICT_AUTOINC_MUTEX));
	} else if (level == SYNC_FOREIGN_KEY_CHECK) {
		ut_a(sync_thread_levels_g(array, SYNC_FOREIGN_KEY_CHECK));
	} else if (level == SYNC_DICT_HEADER) {
		ut_a(sync_thread_levels_g(array, SYNC_DICT_HEADER));
	} else if (level == SYNC_PURGE_IS_RUNNING) {
		ut_a(sync_thread_levels_g(array, SYNC_PURGE_IS_RUNNING));
	} else if (level == SYNC_DICT) {
		ut_a(buf_debug_prints
			|| sync_thread_levels_g(array, SYNC_DICT));
	} else {
		ut_error;
	}

	/*找到对应的层并设置一个latch*/
	for(i = 0; i < SYNC_THREAD_N_LEVELS; i++){
		slot = sync_thread_levels_get_nth(array, i);
		if(slot->latch == NULL){
			slot->latch = latch;
			slot->level = level;
			break;
		}
	}

	ut_a(i < SYNC_THREAD_N_LEVELS);

	mutex_exit(&sync_thread_mutex);
}

ibool sync_thread_reset_level(void* latch)
{
	sync_level_t*	array;
	sync_level_t*	slot;
	sync_thread_t*	thread_slot;
	ulint		i;

	if(!sync_order_checks_on)
		return FALSE;

	if((latch == (void*)&sync_thread_mutex) || (latch == (void*)&mutex_list_mutex)
		|| (latch == (void*)&rw_lock_debug_mutex) || (latch == (void*)&rw_lock_list_mutex))
		reurn FALSE;

	mutex_enter(&sync_thread_mutex);
	/*查找thread slot*/
	thread_slot = sync_thread_level_arrays_find_slot();
	if(thread_slot == NULL){
		ut_error;
		mutex_exit(&sync_thread_mutex);
		return(FALSE);
	}
	/*在level数组上寻找空闲的latch slot*/
	array = thread_slot->levels;
	for(i = 0; i < SYNC_THREAD_N_LEVELS; i ++){
		slot = sync_thread_levels_get_nth(array, i);
		if(slot->latch == latch){
			slot->latch = NULL;
			mutex_exit(&sync_thread_mutex);
			return TRUE;
		}
	}

	ut_error;
	mutex_exit(&sync_thread_mutex);

	return FALSE;
}

void sync_init()
{
	sync_thread_t* thread_slot;
	ulint i;

	ut_a(!sync_initialized);
	sync_initialized = TRUE;

	/*创建一个latch cell array*/
	sync_primary_wait_array = sync_array_create(OS_THREAD_MAX_N, SYNC_ARRAY_OS_MUTEX);
	/*创建latch thread slots*/
	sync_thread_level_arrays = ut_malloc(OS_THREAD_MAX_N *  sizeof(sync_thread_t));
	for(i = 0; i < OS_THREAD_MAX_N; i ++){
		thread_slot = sync_thread_level_arrays_get_nth(i);
		thread_slot = NULL;
	}

	/*mutex全局锁*/
	UT_LIST_INIT(mutex_list);
	mutex_create(&mutex_list_mutex);
	mutex_set_level(&mutex_list_mutex, SYNC_NO_ORDER_CHECK);

	/*rw_lock全局锁*/
	UT_LIST_INIT(rw_lock_list);
	mutex_create(&rw_lock_list_mutex);
	mutex_set_level(&rw_lock_list_mutex, SYNC_NO_ORDER_CHECK);
	mutex_create(&rw_lock_debug_mutex);
	mutex_set_level(&rw_lock_debug_mutex, SYNC_NO_ORDER_CHECK);

	rw_lock_debug_event = os_event_create(NULL);
	rw_lock_debug_waiters = FALSE;
}

void sync_close()
{
	sync_array_free(sync_primary_wait_array);
}

void sync_print_wait_info(char*	buf, char*	buf_end)
{
#ifdef UNIV_SYNC_DEBUG
	printf("Mutex exits %lu, rws exits %lu, rwx exits %lu\n",
		mutex_exit_count, rw_s_exit_count, rw_x_exit_count);
#endif
	if (buf_end - buf < 500) {

		return;
	}

	sprintf(buf,
		"Mutex spin waits %lu, rounds %lu, OS waits %lu\n"
		"RW-shared spins %lu, OS waits %lu; RW-excl spins %lu, OS waits %lu\n",
		mutex_spin_wait_count, mutex_spin_round_count,
		mutex_os_wait_count,
		rw_s_spin_wait_count, rw_s_os_wait_count,
		rw_x_spin_wait_count, rw_x_os_wait_count);
}

void sync_print(char* buf, char* buf_end)
{
	mutex_list_print_info();

	rw_lock_list_print_info();
	sync_array_print_info(buf, buf_end, sync_primary_wait_array);
	buf = buf + strlen(buf);

	sync_print_wait_info(buf, buf_end);
}



















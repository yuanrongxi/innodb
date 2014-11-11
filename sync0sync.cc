#include "sync0sync.h"
#include "sync0rw.h"
#include "buf0buf.h"
#include "sev0srv.h"
#include "buf0types.h"


ulint	sync_dummy				= 0;
ulint	mutex_system_call_count = 0;
ulint	mutex_spin_round_count	= 0;
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
#elif defined(not_defined) && defined(__GNUC__) && defined(UNIV_INTEL_X86)
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
	ibool	ret;

	ret = os_fast_mutex_trylock(&(mutex->os_fast_mutex));

	if (ret == 0) {
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
	return(*ptr);
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

	if (mutex_get_waiters(mutex) != 0)
		mutex_signal_object(mutex);

	mutex_exit_count++;
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


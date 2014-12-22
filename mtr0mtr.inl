#include "sync0sync.h"
#include "sync0rw.h"
#include "mach0data.h"

UNIV_INLINE mtr_t* mtr_start(mtr_t* mtr)
{
	dyn_array_create(&(mtr->memo));
	dyn_array_create(&(mtr->log));

	mtr->log_mode = MTR_LOG_ALL;
	mtr->modifications = FALSE;
	mtr->n_log_recs = 0;

#ifdef UNIV_DEBUG
	mtr->state = MTR_ACTIVE;
	mtr->magic_n = MTR_MAGIC_N;
#endif

	return(mtr);
}

UNIV_INLINE void mtr_memo_push(mtr_t* mtr, void* object, ulint type)
{
	dyn_array_t*		memo;
	mtr_memo_slot_t*	slot;

	ut_ad(object);
	ut_ad(type >= MTR_MEMO_PAGE_S_FIX);	
	ut_ad(type <= MTR_MEMO_X_LOCK);
	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);

	memo = &(mtr->memo);
	slot = dyn_array_push(memo, sizeof(mtr_memo_slot_t));
	slot->object = object;
	slot->type = type;
}

UNIV_INLINE ulint mtr_set_savepoint(mtr_t* mtr)
{
	dyn_array_t* memo;
	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);

	memo = &(mtr->memo);
	return dyn_array_get_data_size(memo);
}
/*释放mtr savepoint出的s-latch*/
UNIV_INLINE void mtr_release_s_latch_at_savepoint(mtr_t* mtr, ulint savepoint, rw_lock_t* lock)
{
	mtr_memo_slot_t*	slot;
	dyn_array_t*		memo;

	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);
	ut_ad(mtr->state == MTR_ACTIVE);

	memo = &mtr->memo;
	ut_ad(dyn_array_get_data_size(memo) > savepoint);
	slot = dyn_array_get_element(memo, savepoint);

	ut_ad(slot->object == lock);
	ut_ad(slot->type == MTR_MEMO_S_LOCK);

	/*释放rw_lock*/
	rw_lock_s_unlock(lock);
	slot->object = NULL;
}

/*判断mtr中是否包含object类型*/
UNIV_INLINE ibool mtr_memo_contains(mtr_t* mtr, void* object, ulint type)
{
	mtr_memo_slot_t*	slot;
	dyn_array_t*		memo;
	ulint				offset;

	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);

	memo = &mtr->memo;
	offset = dyn_array_get_data_size(memo);
	while(offset > 0){
		offset -= sizeof(mtr_memo_slot_t);
		slot = dyn_array_get_element(memo, offset);
		if(object == lot->object && type == slot->type)
			return TRUE;
	}

	return FALSE;
}

UNIV_INLINE ulint mtr_get_log(mtr_t* mtr)
{
	ut_ad(mtr);
	ut_ad(mtr->magic_n == MTR_MAGIC_N);

	return &mtr->log;
}

UNIV_INLINE ulint mtr_get_log_mode(mtr_t* mtr)
{
	ut_ad(mtr);
	ut_ad(mtr->log_mode >= MTR_LOG_ALL);
	ut_ad(mtr->log_mode <= MTR_LOG_SHORT_INSERTS);

	return(mtr->log_mode);
}

/*设置mtr的mode,并返回设置前的mode*/
UNIV_INLINE ulint mtr_set_log_mode(mtr_t* mtr, ulint mode)
{
	ulint	old_mode;

	ut_ad(mtr);
	ut_ad(mode >= MTR_LOG_ALL);
	ut_ad(mode <= MTR_LOG_SHORT_INSERTS);

	if(mode == MTR_LOG_SHORT_INSERTS && old_mode == MTR_LOG_NONE){

	}
	else
		mtr->log_mode = mode;

	ut_ad(old_mode >= MTR_LOG_ALL);
	ut_ad(old_mode <= MTR_LOG_SHORT_INSERTS);

	return old_mode;
}

/*获得s-latch,并将状态保存到memo当中*/
UNIV_INLINE void mtr_s_lock_func(rw_lock_t* lock, char* file, ulint line, mtr_t* mtr)
{
	ut_ad(mtr);
	ut_ad(lock);

	rw_lock_s_lock_func(lock, 0, file, line);
	mtr_memo_push(mtr, lock, MTR_MEMO_S_LOCK);
}


UNIV_INLINE void mtr_x_lock_func(rw_lock_t* lock, char* file, ulint line, mtr_t* mtr)
{
	ut_ad(mtr);
	ut_ad(lock);
	/*获得x latch,并将状态保存到memo当中*/
	rw_lock_x_lock_func(lock, 0, file, line);
	mtr_memo_push(mtr, lock, MTR_MEMO_X_LOCK);
}





#include "hash0hash.h"
#include "ut0rnd.h"
#include "mem0mem.h"

#ifdef UNIV_PFS_MUTEX
UNIV_INTERN mysql_pfs_key_t	hash_table_mutex_key;
#endif /* UNIV_PFS_MUTEX */

#ifdef UNIV_PFS_RWLOCK
UNIV_INTERN mysql_pfs_key_t	hash_table_rw_lock_key;
#endif /* UNIV_PFS_RWLOCK */

UNIV_INLINE hash_cell_t* hash_get_nth_cell(hash_table_t* table, ulint n)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
	ut_ad(n < table->n_cells);

	return (table->array + n);
}

UNIV_INLINE void hash_table_clear(hash_table_t* table)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

	memset(table->array, 0, table->n_cells * sizeof(hash_cell_t));
}

UNIV_INLINE ulint hash_calc_hash(ulint fold, hash_table_t* table)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

	return (ut_hash_ulint(fold, table->n_cells));
}

/*获得锁的索引ID*/
UNIV_INLINE ulint hash_get_sync_obj_index(hash_table_t* table, ulint fold)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
	ut_ad(table->type != HASH_TABLE_SYNC_NONE);
	ut_ad(ut_is_2pow(table->n_sync_obj));

	return (ut_2pow_remainder(hash_calc_hash(fold, table), table->n_sync_obj));
}

UNIV_INLINE mem_heap_t* hash_get_nth_heap(hash_table_t* table, ulint i)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
	ut_ad(table->type != HASH_TABLE_SYNC_NONE);
	ut_ad(i < table->n_sync_obj);

	return (table->heaps[i]);
}

UNIV_INLINE mem_heap_t* hash_get_heap(hash_table_t* table, ulint fold)
{
	ulint i;

	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

	if(table->heap)
		return table->heap;

	i = hash_get_sync_obj_index(table, hash_calc_hash(fold, table));
	return hash_get_nth_heap(table, i);
}

UNIV_INLINE ib_mutex_t* hash_get_nth_mutex(hash_table_t* table, ulint i)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
	ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
	ut_ad(i < table->n_sync_obj);

	return (table->sync_obj.mutexes + i);
}

UNIV_INLINE ib_mutex_t* hash_get_mutex(hash_table_t* table, ulint fold)
{
	ulint i;
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

	i = hash_get_sync_obj_index(table, hash_calc_hash(fold, table));
	return hash_get_nth_mutex(table, i);
}

UNIV_INLINE rw_lock_t* hash_get_nth_lock(hash_table_t* table, ulint i)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
	ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
	ut_ad(i < table->n_sync_obj);

	return (table->sync_obj.rw_locks + i);
}

UNIV_INLINE rw_lock_t* hash_get_lock(hash_table_t* table, ulint fold)
{
	ulint i;
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

	i = hash_get_sync_obj_index(table, hash_calc_hash(fold, table));
	return hash_get_nth_lock(table, i);
}

UNIV_INTERN void hash_mutex_enter(hash_table_t* table, ulint fold)
{
	ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
	mutex_enter(hash_get_mutex(table, fold));
}

UNIV_INTERN void hash_mutex_exit(hash_table_t* table, ulint fold)
{
	ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
	mutex_exit(hash_get_mutex(table, fold));
}

UNIV_INTERN void hash_mutex_enter_all(hash_table_t* table)
{
	ulint i;
	ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
	for(i = 0; i < table->n_sync_obj; i ++)
		mutex_enter(table->sync_obj.mutexes + i);
}

UNIV_INTERN void hash_mutex_exit_all(hash_table_t* table)
{
	ulint i;
	ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
	for(i = 0; i < table->n_sync_obj; i ++)
		mutex_exit(table->sync_obj.mutexes + i);
}

UNIV_INTERN void hash_mutex_exit_all_but(hash_table_t* table, ib_mutex_t* keep_mutex)
{
	ulint i;
	ut_ad(table->type == HASH_TABLE_SYNC_MUTEX);
	for(i = 0; i < table->n_sync_obj; i ++){
		ib_mutex_t* mutex = table->sync_obj.mutexes + i;
		if(UNIV_LIKELY(keep_mutex != mutex))
		mutex_exit(mutex);
	}
}

UNIV_INTERN void hash_lock_s(hash_table_t* table, ulint fold)
{
	rw_lock_t* lock = hash_get_lock(table, fold);
	ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
	ut_ad(lock);

	rw_lock_s_lock(lock);
}

UNIV_INTERN void hash_lock_x(hash_table_t* table, ulint fold)
{
	rw_lock_t* lock = hash_get_lock(table, fold);
	ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
	ut_ad(lock);

	rw_lock_x_lock(lock);
}

UNIV_INTERN void hash_unlock_s(hash_table_t* table, ulint fold)
{
	rw_lock_t* lock = hash_get_lock(table, fold);
	ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
	ut_ad(lock);

	rw_lock_s_unlock(lock);
}

UNIV_INTERN void hash_unlock_x(hash_table_t* table, ulint fold)
{
	rw_lock_t* lock = hash_get_lock(table, fold);
	ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
	ut_ad(lock);

	rw_lock_x_unlock(lock);
}

UNIV_INTERN void hash_unlock_x_all(hash_table_t* table)
{
	ulint i;
	ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
	ut_ad(lock);

	for(i = 0; i < table->n_sync_obj; i ++){
		rw_lock_t* lock = table->sync_obj.rw_locks + i;
		rw_lock_x_lock(lock);
	}
}

UNIV_INTERN void hash_unlock_x_all_put(hash_table_t* table, rw_lock_t* keep_lock)
{
	ulint i;
	ut_ad(table->type == HASH_TABLE_SYNC_RW_LOCK);
	ut_ad(lock);

	for(i = 0; i < table->n_sync_obj; i ++){
		rw_lock_t* lock = table->sync_obj.rw_locks + i;
		if(UNIV_LIKELY(keep_lock != lock))
			rw_lock_x_lock(lock);
	}
}

/*创建一个不带锁的hash*/
UNIV_INTERN hash_table_t* hash_create(ulint n)
{
	hash_cell_t*	array;
	ulint			prime;
	hash_table_t*	table;

	prime = ut_find_prime(n);

	table = static_cast<hash_table_t*>(mem_alloc(sizeof(hash_table_t)));
	array = static_cast<hash_cell_t *>(ut_malloc(sizeof(hash_cell_t) * n));

	table->type = HASH_TABLE_SYNC_NONE;
	table->array = array;
	table->n_cells = prime;

	table->n_sync_obj = 0;
	table->sync_obj.mutexes = NULL;
	table->heaps = NULL;

	table->magic_n = HASH_TABLE_MAGIC_N;

	hash_table_clear(table);

	return table;
}

UNIV_INTERN void hash_table_free(hash_table_t* table)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

	ut_free(table->array);
	mem_free(table);
	/*锁在什么地方释放？*/
}

/*n_sync_obj一定要是2的N次方*/
UNIV_INTERN void hash_create_sync_obj_func(hash_table_t* table, enum hash_table_sync_t type, ulint n_sync_obj)
{
	ulint	i;

	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
	ut_a(n_sync_obj > 0);
	ut_a(ut_is_2pow(n_sync_obj));
	
	table->type = type;

	switch(type){
	case HASH_TABLE_SYNC_MUTEX:
		table->sync_obj.mutexes = static_cast<ib_mutex_t*>(mem_alloc(n_sync_obj * sizeof(ib_mutex_t)));
		for(i = 0; i< n_sync_obj; i ++){
			mutex_create(hash_table_mutex_key, table->sync_obj.mutexes + i, SYNC_MEM_HASH);
		}
		break;

	case HASH_TABLE_SYNC_RW_LOCK:
		table->sync_obj.rw_locks = static_cast<rw_lock_t*>(mem_alloc(n_sync_obj * sizeof(rw_lock_t)));

		for (i = 0; i < n_sync_obj; i++) {
			rw_lock_create(hash_table_rw_lock_key,table->sync_obj.rw_locks + i, SYNC_MEM_HASH);
		}
		break;

	default:
		ut_error;
	}

	table->n_sync_obj = n_sync_obj;
}


#include "thr0loc.h"
#include "sync0sync.h"
#include "hash0hash.h"
#include "mem0mem.h"
#include "sync0types.h"

#define THR_LOCAL_MAGIC_N 1231234

mutex_t thr_local_mutex;
hash_table_t* thr_local_hash = NULL;

typedef struct thr_local_struct thr_local_t;
struct thr_local_struct
{
	os_thread_id_t		id;			/*线程ID*/
	os_thread_t			handle;		/*线程句柄*/
	ulint				slot_no;	/**/
	ibool				in_ibuf;	/*ibuf标识*/
	hash_node_t			hash;
	ulint				magic_n;	/*魔法字*/
};

static thr_local_t* thr_local_get(os_thread_id_t id)
{
	thr_local_t* local;

try_again:
	ut_ad(thr_local_hash);
	ut_ad(mutex_own(&thr_local_mutex));

	local = NULL;
	HASH_SEARCH(hash, thr_local_hash, os_thread_pf(id), local, os_thread_eq(local->id, id));
	if(local == NULL){
		mutex_exit(&thr_local_mutex);
		thr_local_create();
		mutex_enter(&thr_local_mutex);

		goto try_again;
	}

	ut_ad(local_magic_n == THR_LOCAL_MAGIC_N);
	return local;
}

ulint thr_local_get_slot_no(os_thread_id_t id)
{
	ulint			slot_no;
	thr_local_t*	local;

	mutex_enter(&thr_local_mutex);
	local = thr_local_get(id);
	slot_no = local->slot_no;
	mutex_exit(&thr_local_mutex);

	return slot_no;
}

void thr_local_set_slot_no(os_thread_id_t id, ulint	slot_no)
{
	thr_local_t*	local;

	mutex_enter(&thr_local_mutex);
	local = thr_local_get(id);
	local->slot_no = slot_no;
	mutex_exit(&thr_local_mutex);
}

ibool* thr_local_get_in_ibuf_field()
{
	thr_local_t* local;
	
	mutex_enter(&thr_local_mutex);
	local = thr_local_get(os_thread_get_curr_id());
	mutex_exit(&thr_local_mutex);

	return &(local->in_ibuf);
}

void thr_local_create(void)
{
	thr_local_t* local;

	if(thr_local_hash == NULL)
		thr_local_init();

	/*使用mem_heap_t来做内存分配,并将self thread info插入到HASH TABLE当中*/
	local = mem_alloc(sizeof(thr_local_t));

	local->id = os_thread_get_curr_id();
	local->handle = os_thread_get_curr();
	local->magic_n = THR_LOCAL_MAGIC_N;

	local->in_ibuf = FALSE;

	mutex_enter(&thr_local_mutex);
	HASH_INSERT(thr_local_t, hash, thr_local_hash, os_thread_pf(os_thread_get_curr_id()), local);
	mutex_exit(&thr_local_mutex);
}

void thr_local_free(os_thread_id_t	id)
{
	thr_local_t* local;
	mutex_enter(&thr_local_mutex);

	HASH_SEARCH(hash, thr_local_hash, os_thread_pf(id), local, os_thread_eq(local->id, id));
	if(local == NULL){
		mutex_exit(&thr_local_mutex);
		return;
	}
	/*从HASH TABLE中删除thread*/
	HASH_DELETE(thr_local_t, hash, thr_local_hash, os_thread_pf(id), local);

	ut_a(local->magic_n == THR_LOCAL_MAGIC_N);

	mem_free(local);
}

void thr_local_init(void)
{

	ut_a(thr_local_hash == NULL);
	/*创建一个长度OS_THREAD_MAX_N的hash table,尽量让hash node不碰撞*/
	thr_local_hash = hash_create(OS_THREAD_MAX_N + 100);

	mutex_create(&thr_local_mutex);
	mutex_set_level(&thr_local_mutex, SYNC_THR_LOCAL);
}




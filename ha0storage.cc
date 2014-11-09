#include "ha0storage.h"
#include "hash0hash.h"
#include "mem0mem.h"
#include "ut0rnd.h"

/*定义ha_storage_t*/
struct ha_storage_struct
{
	mem_heap_t*		heap;			/*内存分配堆*/
	hash_table_t*	hash;			/*hash table*/
};

typedef struct ha_storage_node_struct ha_storage_node_t;

/*定义ha_storage_t中的hash table的node结构*/
struct ha_storage_node_struct
{
	ulint					data_len;	/*数据长度*/
	const void*				data;		/*数据缓冲区*/
	ha_storage_node_struct* next;		/*下一个节点指针*/
};

UNIV_INLINE ha_storage_t* ha_storage_create(ulint initial_heap_bytes, ulint initial_hash_cells)
{
	ha_storage_t*	storage;
	mem_heap_t*	heap;

	if(initial_hash_cells <= 0)
		initial_hash_cells = HA_STORAGE_DEFAULT_HASH_CELLS;
	
	if(initial_heap_bytes <= 0)
		initial_heap_bytes = HA_STORAGE_DEFAULT_HEAP_BYTES;

	heap = mem_heap_create(sizeof(ha_storage_t) + initial_heap_bytes);

	storage = (ha_storage_t*)mem_heap_alloc(heap, sizeof(ha_storage_t));
	storage->heap = heap;
	storage->hash = hash_create(initial_hash_cells);

	return storage;
}

UNIV_INLINE void ha_storage_empty(ha_storage_t** storage)
{
	ha_storage_t temp_storage;
	temp_storage.heap = (*storage)->heap;
	temp_storage.hash = (*storage)->hash;

	hash_table_clear(temp_storage.hash);
	mem_heap_empty(temp_storage.heap);

	*storage = (ha_storage_t*)mem_heap_alloc(temp_storage.heap, sizeof(ha_storage_t));

	(*storage)->heap = temp_storage.heap;
	(*storage)->hash = temp_storage.hash;
}

UNIV_INLINE void ha_storage_free(ha_storage_t* storage)
{
	hash_table_free(storage->hash);
	mem_heap_free(storage->heap);
}

UNIV_INLINE ulint ha_storage_get_size(const ha_storage_t* storage)
{
	ulint ret;

	ret = mem_heap_get_size(storage->heap);
	ret += sizeof(hash_table_t);
	ret += sizeof(hash_cell_t) * hash_get_n_cells(storage->hash);

	return ret;
}

static const void* ha_storage_get(ha_storage_t* storage, const void* data, ulint data_len)
{
	ha_storage_node_t* node;
	ulint fold;

	fold = ut_fold_binary((unsigned char*)data, data_len);

#define IS_FOUND \
	node->data_len == data_len && memcmp(node->data, data, data_len) == 0

	HASH_SEACH(next, storage->hash, fold, ha_storage_node_t*, node, , IS_FOUND);
	if(node != NULL)
		return node->data;
	
	return NULL;
}

UNIV_INTERN const void* ha_storage_put_memlim(ha_storage_t* storage, const void* data, ulint data_len, ulint memlim)
{
	void*		raw;
	ha_storage_node_t* node;
	const void*	data_copy;
	ulint		fold;

	data_copy = ha_storage_get(storage, data, data_len);
	if(data_copy != NULL)
		return data_copy;

	/*内存超出限制*/
	if(memlim > 0 && ha_storage_get_size(storage) + data_len > memlim)
		return NULL;

	raw = mem_heap_alloc(storage->heap, sizeof(ha_storage_node_t) + data_len);
	node = (ha_storage_node_t*)raw;
	node->data_len = data_len;
	node->data = (byte*)(raw) + sizeof(ha_storage_node_t);
	memcpy((byte*)(raw) + sizeof(ha_storage_node_t), data, data_len);

	fold = ut_fold_binary((unsigned char*)data, data_len);
	HASH_INSERT(ha_storage_node_t, next, storage->hash, fold, node);

	return (byte*)(raw) + sizeof(ha_storage_node_t);
}



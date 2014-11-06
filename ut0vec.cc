#include "ut0vec.h"

#define IB_VEC_OFFSET(v, i) (vec->sizeof_value * i)

UNIV_INLINE void* ib_heap_malloc(ib_alloc_t* allocator, ulint size)
{
	mem_heap_t* heap = (mem_heap_t *)allocator->arg;
	return(mem_heap_alloc(heap, size));
}

/*默认在heap中释放*/
UNIV_INLINE void ib_heap_free(ib_alloc_t* allocator UNIV_UNUSED, void* ptr UNIV_UNUSED)
{

}

UNIV_INLINE void* ib_heap_resize(ib_alloc_t* allocator, void* old_ptr, ulint old_size, ulint new_size)
{
	void* new_ptr;

	mem_heap_t* heap = (mem_heap_t*)allocator->arg;
	new_ptr = mem_heap_alloc(heap, new_size);
	memcpy(new_ptr, old_ptr, old_size);
	/*old ptr在什么地方释放呢？？*/
	return new_ptr;
}

/*分配一个ib_alloc_t*/
UNIV_INLINE ib_alloc_t* ib_heap_allocator_create(mem_heap_t* heap)
{
	ib_alloc_t* heap_alloc;
	heap_alloc = (ib_alloc_t*)mem_heap_alloc(heap, sizeof(ib_alloc_t));
	heap_alloc->arg = heap;
	/*设置分配器函数*/
	heap_alloc->mem_release = ib_heap_free;
	heap_alloc->mem_malloc = ib_heap_malloc;
	heap_alloc->mem_resize = ib_heap_resize;

	return heap_alloc;
}

UNIV_INLINE ib_heap_allocator_free(ib_alloc_t* ib_allocator)
{
	mem_heap_free((mem_heap_t*)ib_allocator->arg);
}

UNIV_INLINE void* ib_ut_malloc(ib_alloc_t* allocator UNIV_UNUSED, ulint size)
{
	return (ut_malloc(size));
}

UNIV_INLINE void ib_ut_free(ib_alloc_t*	allocator UNIV_UNUSED, void* ptr)			
{
	ut_free(ptr);
}

UNIV_INLINE void* ib_ut_resize(ib_alloc_t* allocator UNIV_UNUSED, void* old_ptr, ulint old_size UNIV_UNUSED, ulint new_size)
{
	return ut_realloc(old_ptr, new_size);
}

UNIV_INLINE ib_alloc_t* ib_ut_allocator_create(void)
{
	ib_alloc_t* ib_ut_alloc;
	ib_ut_alloc = malloc(sizeof(ib_ut_alloc));

	ib_ut_alloc->arg = NULL;
	ib_ut_alloc->mem_release = ib_ut_free;
	ib_ut_alloc->mem_malloc = ib_ut_malloc;
	ib_ut_alloc->mem_resize = ib_ut_resize;

	return ib_ut_alloc;
}

UNIV_INTERN ib_vector_t* ib_vector_create(ib_alloc_t* allocator, ulint sizeof_value, ulint size)
{
	ib_vector_t* vec;

	ut_a(size > 0);

	vec = static_cast<ib_vector_t*>(allocator->mem_malloc(allocator, sizeof(ib_vector_t)));
	vec->used = 0;
	vec->total = size;
	vec->allocator = allocator;
	vec->sizeof_value = sizeof_value;

	vec->data = static_cast<void*>(allocator->mem_malloc(allocator, vec->sizeof_value * size));

	return vec;
}

UNIV_INLINE void ib_vector_free(ib_vector_t* vec)
{
	/*根据arg来判断是否是heap方式分配的内存*/
	if(vec->allocator->arg){
		mem_heap_free((mem_heap_t)vec->allocator->arg);
	}
	else{
		ib_alloc_t* allocator;
		allocator = vec->allocator;
		allocator->mem_release(allocator, vec->data);
		allocator->mem_release(allocator, vec);

		ib_ut_allocator_free(allocator);
	}
}

UNIV_INLINE ulint ib_vector_size(const ib_vector_t* vec)
{
	return vec->used;
}

UNIV_INLINE ibool ib_vector_is_empty(const ib_vector_t* vec)
{
	return (ib_vector_size(vec) == 0);
}

/*获取第n个item指针*/
UNIV_INLINE void* ib_vector_get(ib_vector_t* vec, ulint n)
{
	ut_ad(n < vec->used);
	return (byte*)vec->data + IB_VEC_OFFSET(vec, n);
}

UNIV_INLINE const void* ib_vector_get_const(const ib_vector_t* vec, ulint n)
{
	ut_ad(n < vec->used);
	return (byte*)vec->data + IB_VEC_OFFSET(vec, n);
}

UNIV_INLINE void* ib_vector_get_last(ib_vector_t* vec)
{
	ut_a(vec->used > 0);

	return ib_vector_get(vec, vec->used - 1);
}

UNIV_INLINE const void* ib_vector_get_last_const(const ib_vector_t* vec)
{
	ut_a(vec->used > 0);

	return ib_vector_get_const(vec, vec->used - 1);
}

UNIV_INLINE void* ib_vector_push(ib_vector_t* vec, const void* elem)
{
	void* last;

	if(vec->used >= vec->total)
		ib_vector_resize(vec);

	last = ib_vector_get(vec, vec->used);
	if(last != NULL)
		memcpy(last, elem, vec->sizeof_value);

	vec->used ++;

	return last;
}

UNIV_INLINE void* ib_vector_pop(ib_vector_t* vec)
{
	void* elem;
	ut_a(vec->used > 0);

	elem = ib_vector_get_last(vec);
	--vec->used;

	return elem;
}

UNIV_INLINE void* ib_vector_remove(ib_vector_t* vec, const void* elem)
{
	void* current = NULL;
	void* next;
	ulint i;
	ulint old_used_count = vec->used;

	for(i = 0; i < vec->used; i ++){
		current = ib_vector_get(vec, i);

		if(*(void**)current == elem){
			if(i = vec->used - 1)
				return (ib_vector_pop(vec));
			else{
				next = ib_vector_get(vec, i + 1);
				memmove(current, next, vec->sizeof_value * (vec->used - i - 1));
				--vec->used;
				break;
			}
		}
	}

	return ((old_used_count != vec->used) ? current : NULL);
}

UNIV_INLINE void ib_vector_set(ib_vector_t* vec, ulint n, void* elem)
{
	void* slot;
	ut_a(n < vec->used);

	slot = ((byte*)vec->data + IB_VEC_OFFSET(vec, n));
	memcpy(slot, elem, vec->sizeof_value);
}

UNIV_INLINE void ib_vector_sort(ib_vector_t* vec, ib_compare_t compare)
{
	qsort(vec->data, vec->used, vec->sizeof_value, compare);
}

UNIV_INLINE void ib_vector_reset(ib_vector_t* vec)
{
	vec->used = 0;
}

UNIV_INTERN void ib_vector_resize(ib_vector_t* vec)
{
	ulint new_total = vec->total * 2;
	ulint old_size = vec->used * vec->sizeof_value;
	ulint new_size = new_total * vec->sizeof_value;

	vec->data = static_cast<void*>(vec->allocator->mem_resize(vec->allocator, vec->data, old_size, new_size));

	vec->total = new_total;
}




#ifndef __IB_VECTOR_H
#define __IB_VECTOR_H

#include "univ.h"
#include "mem0mem.h"

struct ib_alloc_t;
struct ib_vector_t;

/*定义内存分配的函数指针*/
typedef void* (*ib_mem_alloc_t)(ib_alloc_t* allocator, ulint size);
typedef void (*ib_mem_free_t)(ib_alloc_t* allocator, void* ptr);
typedef void* (*im_mem_resize_t)(ib_alloc_t* allocator, void* ptr, ulint old_size, ulint new_size);
/*内存比较函数指针*/
typedef int (*ib_compare_t)(const void*, const void*);
/*内存分配器*/
struct ib_alloc_t
{
	ib_mem_alloc_t	mem_malloc;
	ib_mem_free_t	mem_release;
	im_mem_resize_t mem_resize;
	void*	arg;
};

/*vector结构*/
struct ib_vector_t
{
	ib_alloc_t*	allocator;		/*内存分配器*/
	void*		data;			/*数据单元列表*/
	ulint		used;			/*当前使用的elem个数*/
	ulint		total;			/*被分配的单元总数*/
	ulint		sizeof_value;	/*单元的数据大小*/
};

#define ib_vector_getp(v, n) (*(void**) ib_vector_get(v, n))
#define ib_vector_getp_const(v, n) (*(void**) ib_vector_get_const(v, n))

/*获得v对应的分配器*/
#define ib_vector_allocator(v) (v->allocator)
/*分配一个vector对象*/
UNIV_INTERN ib_vector_t* ib_vector_create(ib_alloc_t* alloc, ulint sizeof_value, ulint size);
/*释放一个vector*/
UNIV_INLINE void ib_vector_free(ib_vector_t* vec);

UNIV_INLINE void* ib_vector_push(ib_vector_t* vec, const void* elem);

UNIV_INLINE void* ib_vector_pop(ib_vector_t* vec);

UNIV_INLINE void* ib_vector_remove(ib_vector_t* vec, const void* elem);

UNIV_INTERN ulint ib_vector_size(const ib_vector_t* vec);

UNIV_INLINE void ib_vector_resize(const ib_vector_t* vec);

UNIV_INLINE ibool ib_vector_is_empty(const ib_vector_t* vec);

UNIV_INLINE void* ib_vector_get(ib_vector_t* vec, ulint n);

UNIV_INLINE const void* ib_vector_get_const(const ib_vector_t* vec, ulint n);

UNIV_INLINE void* ib_vector_get_last(ib_vector_t* vec);

UNIV_INLINE const void* ib_vector_get_last_const(const ib_vector_t* vec);

UNIV_INLINE void ib_vector_sort(ib_vector_t* vec, ib_compare_t compare);

UNIV_INLINE void ib_vector_set(ib_vector_t* vec, ulint n, void* elem);

UNIV_INLINE void ib_vector_reset(ib_vector_t* vec);
#endif

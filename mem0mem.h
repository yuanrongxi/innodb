#ifndef mem0mem_h
#define mem0mem_h

#include "univ.i"
#include "ut0mem.h"
#include "ut0byte.h"
#include "ut0ut.h"
#include "ut0rnd.h"
#include "sync0sync.h"
#include "ut0lst.h"
#include "mach0data.h"
#include "mem0dbg.h"

#define MEM_HEAP_DYNAMIC	0	/* the most common type */
#define MEM_HEAP_BUFFER		1
#define MEM_HEAP_BTR_SEARCH	2

#define MEM_BLOCK_START_SIZE            64
#define MEM_BLOCK_STANDARD_SIZE         8000

#define MEM_MAX_ALLOC_IN_BUF		(UNIV_PAGE_SIZE - 200)

/*魔法字*/
#define MEM_BLOCK_MAGIC_N	764741555
#define MEM_FREED_BLOCK_MAGIC_N	547711122

#define MEM_BLOCK_HEADER_SIZE ut_calc_align(sizeof(mem_block_info_t), UNIV_MEM_ALIGNMENT)

typedef struct mem_block_info_struct mem_block_info_t;
typedef mem_block_info_t mem_block_t;
typedef mem_block_t	mem_heap_t;

struct mem_block_info_struct
{
	ulint		magic_n;
	char		file_name[8];		/*分配内存的文件*/
	ulint		line;				/*分配内存的文件所在行*/
	ulint		len;				/*block的长度*/
	ulint		type;				/*线程竞争类型*/
	ibool		init_block;			/*是否是外部分配的内存块*/

	ulint		free;				/*被占用的空间大小*/
	ulint		start;						
	byte*		free_block;

	UT_LIST_BASE_NODE_T(mem_block_t)	base;
	UT_LIST_NODE_T(mem_block_t)			list;
};

#define mem_heap_create(N)					mem_heap_create_func((N), NULL, MEM_HEAP_DYNAMIC, __FILE__, __LINE__)
#define mem_heap_create_in_buffer(N)		mem_heap_create_func((N), NULL, MEM_HEAP_BUFFER, __FILE__, __LINE__)
#define mem_heap_create_in_btr_search(N)	mem_heap_create_func((N), NULL, MEM_HEAP_BTR_SEARCH | MEM_HEAP_BUFFER, __FILE__, __LINE__)
#define mem_heap_fast_create(N, B)			mem_heap_create_func((N), (B), MEM_HEAP_DYNAMIC, __FILE__, __LINE__)
#define mem_heap_free(heap)					mem_heap_free_func((heap), __FILE__, __LINE__)
#define mem_alloc(N)						mem_alloc_func((N), __FILE__, __LINE__)
#define mem_alloc_noninline(N)				mem_alloc_func_noninline((N), __FILE__, __LINE__)
#define mem_free(PTR)						mem_free_func((PTR), __FILE__, __LINE__)

void					mem_init(ulint size);

UNIV_INLINE mem_heap_t* mem_heap_create_func(ulint n, void* init_block, ulint type, char* file_name, ulint line);
UNIV_INLINE void		mem_heap_free_func(mem_heap_t* heap, char* file_name, ulint line);
UNIV_INLINE void*		mem_heap_alloc(mem_heap_t* heap, ulint n);
UNIV_INLINE byte*		mem_heap_get_heap_top(mem_heap_t* heap);
UNIV_INLINE void		mem_heap_empty(mem_heap_t* heap);
UNIV_INLINE void*		mem_heap_get_top(mem_heap_t* heap);
UNIV_INLINE void		mem_heap_free_top(mem_heap_t* heap, ulint n);
UNIV_INLINE ulint		mem_heap_get_size(mem_heap_t* heap);

UNIV_INLINE void*		mem_alloc_func(ulint n, char* file_name, ulint line);
void					mem_alloc_func_noninline(ulint n, char* file_name, ulint line);
UNIV_INLINE	void		mem_free_func(void* ptr, char* file_name, ulint line);
UNIV_INLINE void*		mem_realloc(void* buf, ulint n, char* file_name, ulint line);

#endif




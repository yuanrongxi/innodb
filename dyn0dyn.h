#ifndef __DYN0DYN_H_
#define __DYN0DYN_H_

#include "univ.h"
#include "ut0lst.h"
#include "mem0mem.h"

typedef struct dyn_block_struct		dyn_block_t;
typedef dyn_block_t					dyn_array_t;

#define	DYN_ARRAY_DATA_SIZE	512

UNIV_INLINE dyn_array_t*		dyn_array_create(dyn_array_t* arr);

UNIV_INLINE void				dyn_array_free(dyn_array_t* arr);

UNIV_INLINE byte*				dyn_array_open(dyn_array_t* arr, ulint size);

UNIV_INLINE void				dyn_array_close(dyn_array_t* arr, byte* ptr);

UNIV_INLINE void*				dyn_array_push(dyn_array_t* arr, ulint size);

UNIV_INLINE void*				dyn_array_get_element(dyn_array_t* arr, ulint pos);

UNIV_INLINE ulint				dyn_array_get_data_size(dyn_array_t* arr);

UNIV_INLINE dyn_block_t*		dyn_array_get_first_block(dyn_array_t* arr);

UNIV_INLINE dyn_block_t*		dyn_array_get_last_block(dyn_array_t* arr);

UNIV_INLINE dyn_block_t*		dyn_array_get_next_block(dyn_array_t* arr, dyn_block_t* block);

UNIV_INLINE ulint				dyn_block_get_used(dyn_block_t* block);

UNIV_INLINE byte*				dyn_block_get_data(dyn_block_t* block);

UNIV_INLINE void				dyn_push_string(dyn_array_t* arr, byte* str, ulint len);

struct dyn_block_struct
{
	mem_heap_t*				heap;
	ulint					data[DYN_ARRAY_DATA_SIZE];
	UT_LIST_BASE_NODE_T(dyn_block_t) base;
	UT_LIST_NODE_T(dyn_block_t) list;

#ifdef UNIV_DEBUG
	ulint					buf_end;
	ulint					magic_n;
#endif

};
#endif





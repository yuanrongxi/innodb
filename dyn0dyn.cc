#include "dyn0dyn.h"

/*在array的后面增加一块
*/
dyn_block_t* dyn_array_add_block(dyn_array_t* arr)
{
	mem_heap_t*		heap;
	dyn_block_t*	block;

	ut_ad(arr);
	ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);
	
	if(arr->heap == NULL){
		UT_LIST_INIT(arr->base);
		UT_LIST_ADD_FIRST(list, arr->base, arr);
		arr->heap = mem_heap_create(sizeof(dyn_block_t));
	}

	block = dyn_array_get_last_block(arr);
	block->used = block->used | DYN_BLOCK_FULL_FLAG;

	heap = arr->heap;
	block = mem_heap_alloc(heap, sizeof(dyn_block_t));

	block->used = 0;
	UT_LIST_ADD_LAST(list, arr->base, block);

	return block;
}





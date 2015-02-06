#include "btr0sea.h"

#include "buf0buf.h"
#include "page0page.h"
#include "page0cur.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "btr0btr.h"
#include "ha0ha.h"

ulint		btr_search_n_succ	= 0;
ulint		btr_search_n_hash_fail	= 0;

/*因为btr_search_latch_temp和btr_search_sys会频繁调用，单纯的行填充，为了CPU cache命中btr_search_latch_temp*/
byte		btr_sea_pad1[64];
rw_lock_t*	btr_search_latch_temp;

/*单纯的行填充，为了CPU cache命中btr_search_sys*/
byte		btr_sea_pad2[64];
btr_search_sys_t*	btr_search_sys;


#define BTR_SEARCH_PAGE_BUILD_LIMIT		16
#define BTR_SEARCH_BUILD_LIMIT			100


static void btr_search_build_page_hash_index(page_t* page, ulint n_fields, ulint n_bytes, ulint side);

/*检查table的heap是否有空闲空间，如果没有从ibuf中获取新的*/
static void btr_search_check_free_space_in_heap(void)
{
	buf_frame_t*	frame;
	hash_table_t*	table;
	mem_heap_t*		heap;

	ut_ad(!rw_lock_own(&btr_search_latch, RW_LOCK_SHARED) && !rw_lock_own(&btr_search_latch, RW_LOCK_EX));

	table = btr_search_sys->hash_index;
	heap = table->heap;

	if(heap->free_block == NULL){
		frame = buf_frame_alloc(); /*不可以之间用free_block =，有可能会内存泄露*/

		rw_lock_x_lock(&btr_search_latch);

		if(heap->free_block == NULL)
			heap->free_block = frame;
		else /*在buf_frame_alloc和rw_lock_x_lock过程中有可能free_block的值改变了*/
			buf_frame_free(frame);

		rw_lock_x_unlock(&btr_search_latch);
	}
}

/*创建全局的自适应hash索引的对象，包括一个rw_lock和hash table*/
void btr_search_sys_create(ulint hash_size)
{
	btr_search_latch_temp = mem_alloc(sizeof(rw_lock_t));

	rw_lock_create(&btr_search_latch);
	/*建立自适应HASH表*/
	btr_search_sys = mem_alloc(sizeof(btr_search_sys_t));
	btr_search_sys->hash_index = ha_create(TRUE, hash_size, 0, 0);

	rw_lock_set_level(&btr_search_latch, SYNC_SEARCH_SYS);
}

/*创建并初始化一个btr_search_t*/
btr_search_t* btr_search_info_create(mem_heap_t* heap)
{
	btr_search_t*	info;

	info = mem_heap_alloc(heap, sizeof(btr_search_t));
	info->magic_n = BTR_SEARCH_MAGIC_N;

	info->last_search = NULL;
	info->n_direction = 0;
	info->root_guess = NULL;

	info->hash_analysis = 0;
	info->n_hash_potential = 0;

	info->last_hash_succ = FALSE;

	info->n_hash_succ = 0;	
	info->n_hash_fail = 0;	
	info->n_patt_succ = 0;	
	info->n_searches = 0;	

	info->n_fields = 1;
	info->n_bytes = 0;

	/*默认从左到右*/
	info->side = BTR_SEARCH_LEFT_SIDE;

	return info;
}


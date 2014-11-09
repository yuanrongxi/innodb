#include "mem0pool.h"

#include "sync0sync.h"
#include "ut0mem.h"
#include "ut0lst.h"
#include "ut0byte.h"

/*mem area free的标志，这个标志size_and_free的最后一个bit上保存*/
#define MEM_AREA_FREE			1
/*mem_area_t最小的大小必须是2个mem_area_t 8字节对齐后的大小*/
#define MEM_AREA_MIN_SIZE		(2 * MEM_AREA_EXTRA_SIZE)

/*mem_pool_t的结构*/
struct mem_pool_struct
{
	byte*		buf;			/*整体内存的句柄*/
	ulint		size;			/*内存池大小*/
	ulint		reserved;		/*当前分配出去的总内存大小*/
	mutex_t		mutex;			/*多线程互斥量*/
	UT_LIST_BASE_NODE_T(mem_area_t) free_list[64]; /*area_t链表数组*/
};

mem_pool_t*		mem_comm_pool = NULL;
/*超出内存池的最大的次数*/
ulint			mem_out_of_mem_err_msg_count = 0;

void mem_pool_mutex_enter()
{
	mutex_enter(&(mem_comm_pool->mutex));
}

void mem_pool_mutex_exit()
{
	mutex_enter(&(mem_comm_pool->mutex));
}

/*area_size一定是2的N次方，所以最后一位是用来释放标志*/
UNIV_INLINE ulint mem_area_get_size(mem_area_t* area)
{
	return area->size_and_free & ~MEM_AREA_FREE;
}

UNIV_INLINE void mem_area_set_size(mem_area_t* area, ulint size)
{
	area->size_and_free = (area->size_and_free & MEM_AREA_FREE) | (size & MEM_AREA_FREE);
}

UNIV_INLINE ibool mem_area_get_free(mem_area_t* area)
{
	ut_ad(TRUE == MEM_AREA_FREE);
	return area->size_and_free & MEM_AREA_FREE;
}

UNIV_INLINE void mem_area_set_free(mem_area_t* area, ibool free)
{
	ut_ad(TRUE == MEM_AREA_FREE);
	area->size_and_free = (area->size_and_free & ~MEM_AREA_FREE) | free;
}

mem_pool_t* mem_pool_create(ulint size)
{
	mem_pool_t*	pool;
	mem_area_t* area;
	ulint		i;
	ulint		used;

	ut_a(size >= 1024);
	pool = ut_malloc(sizeof(mem_pool_t));
	/*分配pool的内存块*/
	pool->buf = ut_malloc_low(size, FALSE);
	pool->size = size;

	mutex_create(&(pool->mutex));
	mutex_set_level(&(pool->mutex), SYNC_MEM_POOL);

	for(i = 0; i < 64; i++){
		UT_LIST_INIT(pool->free_list[i]);
	}

	used = 0;

	/*构建一个buddy alloc内存系统*/
	while(size - used >= MEM_AREA_MIN_SIZE){
		/*计算应该应该落在free_list数组中的序号*/
		i = ut_2_log(size);
		if(ut_2_exp(i) > size - used)
			i --;

		area = (mem_area_t*)(pool->buf + used);
		mem_area_set_size(area, ut_2_exp(i));
		mem_area_set_free(area, TRUE);

		/*将area加入到对应序号的数组中*/
		UT_LIST_ADD_FIRST(free_list, pool->free_list[i], area);

		used += ut_2_exp(i);
	}

	ut_ad(size>= used);
	pool->reserved = 0;
}

static ibool mem_pool_fill_free_list(ulint i, mem_pool_t* pool)
{
	mem_area_t* area;
	mem_area_t* area2;
	ibool		ret;
	char		err_buf[512];

	ut_ad(mutex_own(&(pool->mutex)));

	/*超过了内存池的最大范围*/
	if(i >= 63){
		if(mem_out_of_mem_err_msg_count % 1000000000 == 0){ /*输出错误日志*/
			ut_print_timestamp(stderr);

			fprintf(stderr,
				"  InnoDB: Out of memory in additional memory pool.\n"
				"InnoDB: InnoDB will start allocating memory from the OS.\n"
				"InnoDB: You may get better performance if you configure a bigger\n"
				"InnoDB: value in the MySQL my.cnf file for\n"
				"InnoDB: innodb_additional_mem_pool_size.\n");
		}

		mem_out_of_mem_err_msg_count ++;

		return FALSE;
	}

	area = UT_LIST_GET_FIRST(pool->free_list[i + 1]);
	if(area == NULL){
		if (UT_LIST_GET_LEN(pool->free_list[i + 1]) > 0) {
			ut_print_timestamp(stderr);

			fprintf(stderr, "  InnoDB: Error: mem pool free list %lu length is %lu\n"
				"InnoDB: though the list is empty!\n",
				i + 1, UT_LIST_GET_LEN(pool->free_list[i + 1]));
		}

		/*递归再到i+1层上找合适的内存块，i越大内存块越大*/
		ret = mem_pool_fill_free_list(i + 1, pool);
		if(!ret)
			return FALSE;

		area = UT_LIST_GET_FIRST(pool->free_list[i + 1]);
	}

	if(UT_LIST_GET_LEN(pool->free_list[i + 1]) == 0){
		ut_sprintf_buf(err_buf, ((byte*)area) - 50, 100);
		fprintf(stderr, "InnoDB: Error: Removing element from mem pool free list %lu\n"
			"InnoDB: though the list length is 0! Dump of 100 bytes around element:\n%s\n",
			i + 1, err_buf);
		ut_a(0);
	}
	/*从上一层的list中删除area*/
	UT_LIST_REMOVE(free_list, pool->free_list[i + 1], area);

	/*在i层进行分裂2个等同大小的area*/
	area2 = (mem_area_t*)(((byte*)area) + ut_2_exp(i));

	mem_area_set_size(area2, ut_2_exp(i));
	mem_area_set_free(area2, TRUE);
	UT_LIST_ADD_FIRST(free_list, pool->free_list[i], area2);

	mem_area_set_size(area, ut_2_exp(i));
	UT_LIST_ADD_FIRST(free_list, pool->free_list[i], area);

	return TRUE;
}

void* mem_area_alloc(ulint size, mem_pool_t* pool)
{
	mem_area_t* area;
	ulint		n;
	ibool		ret;
	char		err_buf[512];

	n = ut_2_log(ut_max(size + MEM_AREA_EXTRA_SIZE, MEM_AREA_MIN_SIZE));

	mutex_enter(&(pool->mutex));
	area = UT_LIST_GET_FIRST(pool->free_list[n]);
	if(area == NULL){
		/*从上层进行分裂得到对应area*/
		ret = mem_pool_fill_free_list(n, pool);
		if(!ret){ /*从pool中分配失败，从os层分配*/
			mutex_exit(&(pool->mutex));
			retrun (ut_malloc(size));
		}

		area = UT_LIST_GET_FIRST(pool->free_list[n]);
	}

	if(!mem_area_get_free(area)){
		ut_sprintf_buf(err_buf, ((byte*)area) - 50, 100);
		fprintf(stderr,
			"InnoDB: Error: Removing element from mem pool free list %lu though the\n"
			"InnoDB: element is not marked free! Dump of 100 bytes around element:\n%s\n",
			n, err_buf);
		ut_a(0);
	}

	if (UT_LIST_GET_LEN(pool->free_list[n]) == 0) {
		ut_sprintf_buf(err_buf, ((byte*)area) - 50, 100);
		fprintf(stderr,
			"InnoDB: Error: Removing element from mem pool free list %lu\n"
			"InnoDB: though the list length is 0! Dump of 100 bytes around element:\n%s\n",
			n, err_buf);
		ut_a(0);
	}

	ut_ad(mem_area_get_size(area) == ut_2_exp(n));	
	mem_area_set_free(area, FALSE);
	/*从空闲链表删除*/
	UT_LIST_REMOVE(free_list, pool->free_list[n], area);
	/*修改已使用的大小*/
	pool->reserved += mem_area_get_size(area);
	mutex_exit(&(pool->mutex));

	/*返回指针*/
	return((void*)(MEM_AREA_EXTRA_SIZE + ((byte*)area))); 
}

UNIV_INLINE mem_area_t* mem_area_get_buddy(mem_area_t* area, ulint size, mem_pool_t* pool)
{
	mem_area_t* buddy;

	/*获得和area相关的伙伴*/
	ut_ad(size != 0);
	/*获得高位伙伴*/
	if(((((byte*)area) - pool->buf) % (2 * size)) == 0){
		buddy = (mem_area_t*)(((byte*)area) + size);
		if((((byte*)buddy) - pool->buf) + size > pool->size){ /*内存超出pool的大小*/
			buddy = NULL;
		}
	}
	else{/*获得低位伙伴*/
		buddy = (mem_area_t*)(((byte*)area) - size);
	}

	return buddy;
}

void mem_area_free(void* ptr, mem_pool_t* pool)
{
	mem_area_t* area;
	mem_area_t* buddy;
	void*		new_ptr;
	ulint		size;
	ulint		n;
	char		err_buf[512];

	/*有分配out of memory，就必须判断是否是从pool中分配的，如果不是，需要用系统的free进行释放*/
	if(mem_out_of_mem_err_msg_count > 0){
		if ((byte*)ptr < pool->buf || (byte*)ptr >= pool->buf + pool->size){ /*不在pool内存范围，直接释放*/
				ut_free(ptr);
				return;
		}
	}

	/*获得area句柄*/
	area = (mem_area_t*)(((byte*)ptr) - MEM_AREA_EXTRA_SIZE);
	if(mem_area_get_free(area)){ /*状态不对*/
		ut_sprintf_buf(err_buf, ((byte*)area) - 50, 100);
		fprintf(stderr,
			"InnoDB: Error: Freeing element to mem pool free list though the\n"
			"InnoDB: element is marked free! Dump of 100 bytes around element:\n%s\n",
			err_buf);
		ut_a(0);
	}

	size = mem_area_get_size(area);
	/*获得area的伙伴*/
	buddy = mem_area_get_buddy(area, size, pool);
	n = ut_2_log(size);

	mutex_enter(&(pool->mutex));
	if(buddy != NULL && mem_area_get_free(buddy) && (size == mem_area_get_size(buddy))){ /*buddy是一个空闲的area, 进行area合并放入到上一层*/
		if((byte*)buddy < (byte*)area){
			new_ptr = ((byte*)buddy) + MEM_AREA_EXTRA_SIZE;
			mem_area_set_size(buddy, 2 * size);
			mem_area_set_free(buddy, FALSE);
		}
		else{
			new_ptr = ptr;
			mem_area_set_size(area, 2 * size);
		}
		/*在本层删除buddy*/
		UT_LIST_REMOVE(free_list, pool->free_list[n], buddy);
		/*在合并的时候，area是不在free list中，所以这里是被占用的*/
		pool->reserved += ut_2_exp(n);
		mutex_exit(&(pool->mutex));
		/*合并到上层，并继续释放*/
		mem_area_free(new_ptr, pool);
	}
	else{ /*伙伴是被使用的*/
		UT_LIST_ADD_FIRST(free_list, pool->free_list[n], area);
		mem_area_set_free(area, TRUE);
		ut_ad(pool->reserved >= size);
		pool->reserved -= size;
	}
	mutex_exit(&(pool->mutex));
	/*pool的安全检查*/
	ut_ad(mem_pool_validate(pool));
}

ibool mem_pool_validate(mem_pool_t* pool)
{
	mem_pool_t* pool;
	mem_area_t* buddy;
	ulint		i;
	ulint		free;

	mutex_enter(&(pool->mutex));
	free = 0;

	for(i = 0; i < 64; i++){
		UT_LIST_VALIDATE(free_list, mem_area_t, pool->free_list[i]);
		area = UT_LIST_GET_FIRST(pool->free_list[i]);
		while(area != NULL){
			ut_a(mem_area_get_free(area));
			ut_a(mem_area_get_size(area) == ut_2_exp(i));

			buddy = mem_area_get_buddy(area, ut_2_exp(i), pool);
			ut_a(!buddy || !mem_area_get_free(buddy) || (ut_2_exp(i) != mem_area_get_size(buddy))); /*如果自己和伙伴都是free状态，就是有问题的*/
			area = UT_LIST_GET_NEXT(free_list, area);
			free += ut_2_exp(i);
		}
	}

	/*长度判断*/
	ut_a(free + pool->reserved == pool->size - (pool->size % MEM_AREA_MIN_SIZE));
}

ulint mem_pool_get_reserved(mem_pool_t* pool)
{
	ulint reserved;

	mutex_enter(&(pool->mutex));
	reserved = pool->reserved;
	mutex_exit(&(pool->mutex));

	return reserved;
}



void mem_pool_print_info(FILE* outfile, mem_pool_t*	pool)
{
	ulint i;
	mem_pool_validate(pool);

	fprintf(outfile, "INFO OF A MEMORY POOL\n");

	mutex_enter(&(pool->mutex));
	for (i = 0; i < 64; i++) {
		if (UT_LIST_GET_LEN(pool->free_list[i]) > 0) {
			fprintf(outfile, "Free list length %lu for blocks of size %lu\n", UT_LIST_GET_LEN(pool->free_list[i]), ut_2_exp(i));
		}	
	}

	fprintf(outfile, "Pool size %lu, reserved %lu.\n", pool->size, pool->reserved);
	mutex_exit(&(pool->mutex));
}



















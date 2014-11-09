#include "ut0mem.h"

#include "mem0mem.h"
#include "os0sync.h"
#include "ut0lst.h"

typedef struct ut_mem_block_struct ut_mem_block_t;

ulint ut_total_allocated_memory	= 0;

#define UT_MEM_MAGIC_N 1601650166

struct ut_mem_block_struct
{
	ulint	size;
	ulint	magic_n;
	UT_LIST_NODE_T(ut_mem_block_t) mem_block_list;
};

/*定义一个全局的mem block链表*/
UT_LIST_BASE_NODE_T(ut_mem_block_t)   ut_mem_block_list;

os_fast_mutex_t	ut_list_mutex;
ibool ut_mem_block_list_inited = FALSE;
ulint ut_mem_null_ptr = NULL;

UNIV_INLINE void* ut_memcpy(void* dest, void* src, ulint size)
{
	return memcpy(dest, src, size);
}

UNIV_INLINE void* ut_memmove(void* dest, void* src, ulint n)
{
	return memmove(dest, src, n);
}

UNIV_INLINE int ut_memcmp(void* str1, void* str2, ulint n)
{
	return memcmp(str1, str2, n);
}

UNIV_INLINE char* ut_strcpy(char* dest, char* sour)
{
	return(strcpy(dest, sour));
}

UNIV_INLINE ulint ut_strlen(char* str)
{
	return(strlen(str));
}

UNIV_INLINE int ut_strcmp(void* str1, void* str2)
{
	return(strcmp((char*)str1, (char*)str2));
}

static void ut_mem_block_list_init()
{
	os_fast_mutex_init(&ut_list_mutex);
	UT_LIST_INIT(ut_mem_block_list);
	ut_mem_block_list_inited = TRUE;
}

void* ut_malloc_low(ulint n, ibool set_to_zero)
{
	void* ret;
	ut_ad((sizeof(ut_mem_block_t) % 8) == 0);

	/*如果未初始化，进行初始化*/
	if (!ut_mem_block_list_inited)
		ut_mem_block_list_init();

	os_fast_mutex_lock(&ut_list_mutex);

	ret = malloc(n + sizeof(ut_mem_block_t));
	if (ret == NULL) {
		fprintf(stderr,
			"InnoDB: Fatal error: cannot allocate %lu bytes of\n"
			"InnoDB: memory with malloc! Total allocated memory\n"
			"InnoDB: by InnoDB %lu bytes. Operating system errno: %d\n"
			"InnoDB: Cannot continue operation!\n"
			"InnoDB: Check if you should increase the swap file or\n"
			"InnoDB: ulimits of your operating system.\n"
			"InnoDB: On FreeBSD check you have compiled the OS with\n"
			"InnoDB: a big enough maximum process size.\n"
			"InnoDB: We now intentionally generate a seg fault so that\n"
			"InnoDB: on Linux we get a stack trace.\n",
			n, ut_total_allocated_memory, errno);

		os_fast_mutex_unlock(&ut_list_mutex);

		/* Make an intentional seg fault so that we get a stack
		trace */
		printf("%lu\n", *ut_mem_null_ptr);	
	}

	if(set_to_zero){
#ifdef UNIV_SET_MEM_TO_ZERO
		memset(ret, 0, n + sizeof(ut_mem_block_t));
#endif
	}

	((ut_mem_block_t*)ret)->size = n + sizeof(ut_mem_block_t);
	((ut_mem_block_t*)ret)->magic_n = UT_MEM_MAGIC_N;

	ut_total_allocated_memory += n + sizeof(ut_mem_block_t);

	UT_LIST_ADD_FIRST(mem_block_list, ut_mem_block_list, ((ut_mem_block_t*)ret));

	os_fast_mutex_unlock(&ut_list_mutex);
	return (void *)((byte*)ret + sizeof(ut_mem_block_t));
}

void* ut_malloc(ulint n)
{
	return ut_malloc_low(n, TRUE);
}

void* ut_free(void* ptr)
{
	ut_mem_block_t* block;
	block = (ut_mem_block_t*)((byte*)ptr - sizeof(ut_mem_block_t));
	os_fast_mutex_lock(&ut_list_mutex);

	ut_total_allocated_memory -= block->size;
	UT_LIST_REMOVE(mem_block_list, ut_mem_block_list, block);
	free(block);

	os_fast_mutex_unlock(&ut_list_mutex);
}

/*释放全部的BLOCK*/
void ut_free_all_mem(void)
{
	ut_mem_block_t* block;

	os_fast_mutex_lock(&ut_list_mutex);
	while ((block = UT_LIST_GET_FIRST(ut_mem_block_list))) {
		ut_a(block->magic_n == UT_MEM_MAGIC_N);
		ut_a(ut_total_allocated_memory >= block->size);

		ut_total_allocated_memory -= block->size;

		UT_LIST_REMOVE(mem_block_list, ut_mem_block_list, block);
		free(block);
	}

	os_fast_mutex_unlock(&ut_list_mutex);

	ut_a(ut_total_allocated_memory == 0);
}

/*拼接字符串到一个str里面，这里会malloc内存*/
char* ut_str_catenate(char*	str1, char*	str2)
{
	ulint	len1;
	ulint	len2;
	char*	str;

	len1 = ut_strlen(str1);
	len2 = ut_strlen(str2);

	str = mem_alloc(len1 + len2 + 1);

	ut_memcpy(str, str1, len1);
	ut_memcpy(str + len1, str2, len2);

	str[len1 + len2] = '\0';

	return(str);
}



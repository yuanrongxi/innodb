#ifndef __UT0MEM_H_
#define __UT0MEM_H_

#include "univ.h"
#include <string.h>
#include <stdlib.h>

/*总的内存分配数*/
extern ulint ut_total_allocated_memory;

/*内存操作函数*/
UNIV_INLINE void* ut_memcpy(void* dest, void* src, ulint n);

UNIV_INLINE void* ut_memmove(void* dest, void* src, ulint n);

UNIV_INLINE int ut_memcmp(void* str1, void* str2, ulint n);

/*内存分配函数*/
void* ut_malloc_low(ulint n, ibool set_to_zero);

void* ut_malloc(ulint n);

void ut_free(void* ptr);

void ut_free_all_mem(void);

UNIV_INLINE char* ut_strcpy(char* dest, char* src);

UNIV_INLINE ulint ut_strlen(char* str);

UNIV_INLINE int ut_strcmp(void* str1, void* str2);

char* ut_str_catenate(char* str1, char* str2);

#endif






/**********************************************
*替换算法实现，替换算法是基于buf_block_t对象的
*淘汰机制,LRU的并发控制依赖于buf_pool->mutex。
**********************************************/
#ifndef __buf0lru_h_
#define __buf0lru_h_

#include "univ.h"
#include "ut0byte.h"
#include "buf0types.h"

#define BUF_LRU_OLD_MIN_LEN		80
#define BUF_LRU_FREE_SEARCH_LEN (5 + 2 * BUF_READ_AHEAD_AREA)


void							buf_LRU_try_free_flushed_blocks();
ulint							buf_LRU_get_recent_limit();
buf_block_t*					buf_LRU_get_free_block();
void							buf_LRU_block_free_non_file_page(buf_block_t* block);
void							buf_LRU_add_block(buf_block_t* block, ibool old);
void							buf_LRU_make_block_young(buf_block_t* block);
void							buf_LRU_search_and_free_block(ulint n_iterations);

ibool							buf_LRU_validate();
void							buf_LRU_print();

#include "buf0lru.inl"

#endif

#ifndef __HA0HA_H_
#define __HA0HA_H_

/*这个文件的实现和具体的数据记录和PAGE有很大关系*/
#include "univ.h"

#include "hash0hash.h"
#include "page0types.h"
#include "buf0types.h"
#include "rem0types.h"

UNIV_INLINE const rec_t* ha_search_and_get_data(hash_table_t* table, ulint fold);

UNIV_INLINE void ha_search_and_update_if_found_func(hash_table_t* table, ulint fold, const rec_t* data, const rec_t* new_data);

#define ha_search_and_update_if_found(table,fold,data,new_block,new_data) ha_search_and_update_if_found_func(table,fold,data,new_data)

UNIV_INTERN hash_table_t* ha_create_func(ulint n, ulint n_mutexes);

#define ha_create(n_c,n_m,level) ha_create_func(n_c,n_m)

UNIV_INTERN ibool ha_insert_for_fold_func(hash_table_t*	table, ulint fold, const rec_t*	data);

#define ha_insert_for_fold(t,f,b,d) ha_insert_for_fold_func(t,f,d)

UNIV_INTERN ibool ha_search_and_delete_if_found(hash_table_t* table, ulint fold, const rec_t* data);

UNIV_INTERN void ha_remove_all_nodes_to_page(hash_table_t* table, ulint fold, const page_t* page);

UNIV_INTERN void ha_print_info(FILE* file, hash_table_t* table);

typedef struct ha_node_struct ha_node_t;

struct ha_node_struct
{
	ha_node_t*		next;	/*下一个节点*/
	const rec_t*	data;	/*记录数据指针*/
	ulint			fold;	/*hash key*/
};

#define ASSERT_HASH_MUTEX_OWN(table, fold)	ut_ad(!(table)->mutexes || mutex_own(hash_get_mutex(table, fold)))

#endif









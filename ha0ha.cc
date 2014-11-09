#include "ha0ha.h"
#include "btr0sea.h"
#include "page0page.h"

#include "ut0rnd.h"
#include "mem0mem.h"
#include "btr0types.h"

UNIV_INTERN void ha_delete_hash_node(hash_table_t* table, ha_node_t* node);

UNIV_INLINE const rec_t* ha_node_get_data(const ha_node_t* node)
{
	return node->data;
}

UNIV_INLINE void ha_node_set_data_func(ha_node_t* node, const rec_t* data)
{
	node->data = data;
}

#define ha_node_set_data(n,b,d) ha_node_set_data_func(n,d)

UNIV_INLINE ha_node_t* ha_chain_get_next(ha_node_t* node)
{
	return node->next;
}

UNIV_INLINE ha_node_t* ha_chain_get_first(hash_table_t* table, ulint fold)
{
	hash_cell_t* cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
	if(cell != NULL)
		return (ha_node_t*)cell->node;

	return NULL;
}

UNIV_INLINE const rec_t* ha_search_and_get_data(hash_table_t* table, ulint fold)
{
	ha_node_t* node;

	ASSERT_HASH_MUTEX_OWN(table, fold);
	node = ha_chain_get_first(table, fold);
	
	while(node){
		if(node->fold == fold)
			return node->data;

		node = ha_chain_get_next(node);
	}

	return NULL;
}

UNIV_INLINE ha_node_t* ha_search_with_data(hash_table_t* table, ulint fold, const rec_t* data)
{
	ha_node_t* node;

	ASSERT_HASH_MUTEX_OWN(table, fold);
	node = ha_chain_get_first(table, fold);

	while(node != NULL){
		if(node->data == data)
			return node;

		node = ha_chain_get_next(node);
	}

	return NULL;
}

UNIV_INLINE ibool ha_search_and_delete_if_found(hash_table_t* table, ulint fold, const rec_t* data)
{
	ha_node_t* node;

	ASSERT_HASH_MUTEX_OWN(table, fold);
	node = ha_search_with_data(table, fold, data);
	if(node){
		ha_delete_hash_node(table, node);
		return TRUE;
	}

	return FALSE;
}

UNIV_INTERN hash_table_t* ha_create_func(ulint n, ulint n_mutexes)
{
	hash_table_t* table;
	ulint i;

	ut_ad(ut_is_2pow(n_mutexes));
	table = hash_create(n);

	/*hash的n_sync_obj =0，这个时候必须建立一个hash_table的全局*/
	if(n_mutexes == 0){
		table->heap = mem_heap_create_in_btr_search(ut_min(4096, MEM_MAX_ALLOC_IN_BUF));
		ut_a(table->heap);

		return(table);
	}
	
	/*如果n_mutexes > 0,先建立cell粒度线程锁*/
	hash_create_sync_obj(table, HASH_TABLE_SYNC_MUTEX, 0);

	/*建立对应的堆序列*/
	for (i = 0; i < n_mutexes; i++) {
		table->heaps[i] = mem_heap_create_in_btr_search(4096);
		ut_a(table->heaps[i]);
	}

	return table;
}

UNIV_INTERN ibool ha_insert_for_fold_func(hash_table_t* table, ulint fold, const rec_t* data)
{
	hash_cell_t* cell;
	ha_node_t* node;
	ha_node_t* prev_node;
	ulint hash;

	/*检查fold是否存在于hash table中，如果存在，直接update*/
	hash = hash_calc_hash(fold, table);
	cell = hash_get_nth_cell(table, hash);

	prev_node = (ha_node_t*)(cell->node);
	while(prev_node != NULL){
		if(prev_node->fold == fold){
			prev_node->data = data;
			return TRUE;
		}

		prev_node = prev_node->next;
	}

	node = mem_heap_alloc(hash_get_heap(table, fold), sizeof(ha_node_t));
	if(node == NULL){
		ut_ad(hash_get_heap(table, fold)->type & MEM_HEAP_BTR_SEARCH);
		return(FALSE);
	}
	/*对node赋值*/
	ha_node_set_data(node, block, data);
	node->fold = fold;
	node->next = NULL;

	prev_node = (ha_node_t*)(cell->node);
	if(prev_node == NULL){
		cell->node = node;
		return TRUE;
	}
	/*插入到队列的最后面*/
	while(prev_node->next != NULL){
		prev_node = prev_node->next;
	}

	prev_node->next = node;

	return TRUE;
}

UNIV_INTERN void ha_delete_hash_node(hash_table_t* table, ha_node_t* del_node)
{
	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

	HASH_DELETE_AND_COMPACT(ha_node_t, next, table, del_node);
}

UNIV_INTERN void ha_search_and_update_if_found_func(hash_table_t* table, ulint fold, const rec_t* data, const rec_t* new_data)
{
	hash_table_t* node;

	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);

	if (!btr_search_enabled) {
		return;
	}

	node = ha_search_with_data(table, fold, data);
	if(node != NULL)
		node->data = new_data;
}

UNIV_INTERN void ha_remove_all_nodes_to_page(hash_table_t* table, ulint	fold, const page_t*	page)
{
	ha_node_t*	node;

	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
	ASSERT_HASH_MUTEX_OWN(table, fold);

	ut_ad(btr_search_enabled);
	/*找到对应的cell node*/
	node = ha_chain_get_first(table, fold);
	/*删除page中所有的node*/
	while (node) {
		if (page_align(ha_node_get_data(node)) == page) {
			ha_delete_hash_node(table, node);

			node = ha_chain_get_first(table, fold);
		} else {
			node = ha_chain_get_next(node);
		}
	}
}

UNIV_INTERN void ha_print_info(FILE* file, hash_table_t* table)
{
#ifdef PRINT_USED_CELLS
	hash_cell_t*	cell;
	ulint		cells	= 0;
	ulint		i;
#endif /* PRINT_USED_CELLS */
	ulint		n_bufs;

	ut_ad(table);
	ut_ad(table->magic_n == HASH_TABLE_MAGIC_N);
#ifdef PRINT_USED_CELLS
	for (i = 0; i < hash_get_n_cells(table); i++) {
		cell = hash_get_nth_cell(table, i);
		if (cell->node)
			cells++;
	}
#endif /* PRINT_USED_CELLS */

	fprintf(file, "Hash table size %lu", (ulong) hash_get_n_cells(table));

#ifdef PRINT_USED_CELLS
	fprintf(file, ", used cells %lu", (ulong) cells);
#endif /* PRINT_USED_CELLS */

	if (table->heaps == NULL && table->heap != NULL) {
		n_bufs = UT_LIST_GET_LEN(table->heap->base) - 1;
		if (table->heap->free_block)
			n_bufs++;

		fprintf(file, ", node heap has %lu buffer(s)\n", (ulong) n_bufs);
	}
}

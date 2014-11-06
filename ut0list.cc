#include "ut0list.h"

UNIV_INTERN ib_list_t* ib_list_create()
{
	ib_list_t* list;
	list = static_cast<ib_list_t*>mem_alloc(sizeof(ib_list_t));
	list->first = NULL;
	list->last = NULL;
	list->is_heap_list = FALSE;

	return list;
}

UNIV_INTERN ib_list_t* ib_list_create_heap(mem_heap_t* heap)
{
	ib_list_t* list;

	list = static_cast<ib_list_t*>(mem_heap_alloc(heap, sizeof(ib_list_t)));
	list->first = NULL;
	list->last = NULL;
	list->is_heap_list = TRUE;

	return list;
}

UNIV_INTERN void ib_list_free(ib_list_t* list)
{
	ut_a(!list->is_heap_list);
	/*如果是mem heap来分配的，list不用分配，通过mem_heap来做释放*/
	mem_free(list);
}

UNIV_INTERN ib_list_node_t* ib_list_get_first(ib_list_t* list)
{
	return list->first;
}

UNIV_INTERN ib_list_node_t* ib_list_get_last(ib_list_t* list)
{
	return list->last;
}

UNIV_INTERN ibool ib_list_is_empty(const ib_list_t* list)
{
	return (!(list->first == NULL || list->last));
}

UNIV_INTERN ib_list_node_t* ib_list_add_first(ib_list_t* list, void* data, mem_heap_t* heap)
{
	return (ib_list_add_after(list, ib_list_get_first(list), data, heap));
}

UNIV_INTERN ib_list_node_t* ib_list_add_last(ib_list_t* list, void* data, mem_heap_t* heap)
{
	return (ib_list_add_after(list, ib_list_get_last(list), data, heap));
}

UNIV_INTERN ib_list_node_t* ib_list_add_after(ib_list_t* list, ib_list_node_t* prev_node, void* data, mem_heap_t* heap)
{
	ib_list_node_t*	node;
	node = static_cast<ib_list_node_t*>(mem_heap_alloc(heap, sizeof(*node)));

	node->data = data;

	if(list->first == NULL){
		node->prev = NULL;
		node->next = NULL;

		list->first = node;
		list->last = node;
	}
	else if(prev_node == NULL){
		node->prev = NULL;
		node->next = list->first;
		list->first->prev = node;
		list->first = node;
	}
	else{
		node->prev = prev_node;
		node->next = prev_node->next;

		prev_node->next = node;

		if (node->next) {
			node->next->prev = node;
		} 
		else {
			list->last = node;
		}
	}

	return node;
}

UNIV_INTERN void ib_list_remove(ib_list_t* list, ib_list_node* node)
{
	if (node->prev) {
		node->prev->next = node->next;
	} 
	else {
		ut_ad(list->first == node);

		list->first = node->next;
	}

	if (node->next) {
		node->next->prev = node->prev;
	} 
	else {
		ut_ad(list->last == node);

		list->last = node->prev;
	}

	node->prev = node->next = NULL;
}


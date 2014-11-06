#ifndef IB_LIST_H
#define IB_LIST_H

#include "univ.h"
#include "mem0mem.h"

/*list结构*/
struct ib_list_t
{
	ib_list_node_t* first;
	ib_list_node_t* last;
	ibool			is_heap_list; /*是否通过mem heap分配内存*/
};

/*list的节点数据结构*/
struct ib_list_node_t
{
	ib_list_node_t* prev;		/*上一个节点*/
	ib_list_node_t* next;		/*下一个节点*/
	void*			data;		/*数据指针*/
};

struct ib_list_helper_t
{
	mem_heap_t* heap; /*内存池堆*/
	void*		data;
};

UNIV_INTERN ib_list_t* ib_list_create(void);
UNIV_INTERN ib_list_t* ib_list_create_heap(mem_heap_t* heap);

UNIV_INTERN void ib_list_free(ib_list_t* list);

UNIV_INTERN ib_list_node_t* ib_list_add_first(ib_list_t* list, void* data, mem_heap_t* heap);
UNIV_INTERN ib_list_node_t* ib_list_add_last(ib_list_t* list, void* data, mem_heap_t* heap);
UNIV_INTERN ib_list_node_t* ib_list_add_after(ib_list_t* list, ib_list_node_t* prev_node, void* data, mem_heap_t* heap);

UNIV_INTERN void ib_list_remove(ib_list_t* list, ib_list_node_t* node);

UNIV_INTERN ib_list_node_t* ib_list_get_first(ib_list_t* list);
UNIV_INTERN ib_list_node_t* ib_list_get_last(ib_list_t* list);

UNIV_INTERN ibool ib_list_is_empty(const ib_list_t* list);


#endif



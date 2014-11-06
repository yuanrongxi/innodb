#ifndef ut0lst_h
#define ut0lst_h

#include "univ.h"

/*计算结构体F元素的偏移*/
#define IB_OFFSETOF(T, F) (reinterpret_cast<byte*>(&(T)->F) - reinterpret_cast<byte*>(T))

/*list基础结构*/
template<typename TYPE>
struct ut_list_base
{
	typedef TYPE elem_type;
	ulint count;
	TYPE* start;
	TYPE* end;
};

/*定义list类型的宏，例如UT_LIST_BASE_NODE_T(node_t) node_list;*/
#define UT_LIST_BASE_NODE_T(TYPE) ut_list_base<TYPE>

template<typename TYPE>
struct ut_list_node
{
	TYPE* prev;
	TYPE* next;
};

/*定义list_node类型的宏*/
#define UT_LIST_NODE_T(T) ut_list_node<T>

template<typename T>
ut_list_node<T>& ut_elem_get_node(T& elem, size_t offset)
{
	ut_a(offset < sizeof(elem));
	return(*reinterpret_cast<ut_list_node<T>*>(reinterpret_cast<byte*>(&elem) + offset));
}

/*初始化list宏*/
#define UT_LIST_INIT(BASE) \
{\
	(BASE).count = 0;\
	(BASE).start = NULL;\
	(BASE).end = NULL;\
}

template<typename List, typename Type>
void ut_list_prepend(List& list, Type& elem, size_t offset)
{
	/*找到一个空闲node,elem是个连续内存数组？*/
	ut_list_node<Type>& elem_node = ut_elem_get_node(elem, offset);
	elem_node.prev = NULL;
	elem_node.next = list.start;

	if(list.start != NULL){
		ut_list_node<Type>&	base_node = ut_elem_get_node(*list.start, offset);

		ut_ad(list.start != &elem);
		base_node.prev = &elem;
	}

	list.start = &elem;
	if(list.end == 0)
		list.end = &elem;

	++list.count;
}

#define UT_LIST_ADD_FIRST(NAME, LIST, ELEM) ut_list_append(LIST, *ELEM, IB_OFFSETOF(ELEM, NAME))

/*在链表后面插入一个单元*/
template <typename List, typename Type>
void ut_list_append(List& list, Type& elem, size_t offset)
{
	ut_list_node<Type>&	elem_node = ut_elem_get_node(elem, offset);

	elem_node.next = NULL;
	elem_node.prev = list.end;

	if (list.end != 0){
		ut_list_node<Type>&	base_node = ut_elem_get_node(*list.end, offset);

		ut_ad(list.end != &elem);
		base_node.next = &elem;
	}

	list.end = &elem;

	if (list.start == 0)
		list.start = &elem;


	++list.count;
}

#define UT_LIST_ADD_LAST(NAME, LIST, ELEM) ut_list_append(LIST, *ELEM, IB_OFFSETOF(ELEM, NAME))

/*插入一个单元,在elem1后面插入elem2*/
template<typename List, typename Type>
void ut_list_insert(List& list, Type& elem1, Type& elem2, size_t offset)
{
	ut_ad(&elem1 != &elem2);

	ut_list_node<Type>&	elem1_node = ut_elem_get_node(elem1, offset);
	ut_list_node<Type>&	elem2_node = ut_elem_get_node(elem2, offset);

	elem2_node.prev = &elem1;
	elem2_node.next = elem1_node.next;
	
	if(elem1_node.next != NULL){
		ut_list_node<Type>& next_node = ut_elem_get_node(*elem1_node.next, offset);
		next_node.prev = &elem2;
	}

	elem1_node.next = &elem2;
	if(list.end = &elem1){
		list.end = &elem2;
	}

	++list.count;
}
/*在elem1后面插入elem2*/
#define UT_LIST_INSERT_AFTER(NAME, LIST, ELEM1, ELEM2) ut_list_insert(LIST, *ELEM1, *ELEM2, IB_OFFSETOF(ELEM1, NAME))

#ifdef UNIV_LIST_DEBUG
# define UT_LIST_REMOVE_CLEAR(N)					\
	(N).next = (Type*) -1;						\
	(N).prev = (N).next
#else

# define UT_LIST_REMOVE_CLEAR(N)
#endif /* UNIV_LIST_DEBUG */

/*删除一个单元*/
template <typename List, typename Type>
void ut_list_remove(List& list, Type& elem, size_t offset)
{
	ut_list_node<Type>& elem_node = ut_elem_get_node(elem, offset);

	ut_a(list.count > 0);
	if(elem_node.next != NULL){
		ut_list_node<Type>& next_node = ut_elem_get_node(*elem_node->next, offset);
		next_node.prev = elem_node.prev;
	}
	else
		list.end = elem_node.prev;

	if(elem_node.prev != NULL){
		ut_list_node<Type>& prev_node = ut_elem_get_node(*elem_node->prev, offset);
		prev_node.next = elem_node.next;
	}
	else
		list.start = elem_node.next;

	UT_LIST_REMOVE_CLEAR(elem_node);

	--list.count;
}

#define UT_LIST_REMOVE(NAME, LIST, ELEM) ut_list_remove(LIST, *ELEM, IB_OFFSETOF(ELEM, NAME))

/*获得下NAME指向的一个单元*/
#define UT_LIST_GET_NEXT(NAME, N) (((N)->NAME).next)

#define UT_LIST_GET_PREV(NAME, N) (((N)->NAME).prev)

#define UT_LIST_GET_LEN(BASE) (BASE).count

#define UT_LIST_GET_FIRST(BASE) (BASE).start

#define UT_LIST_GET_LAST(BASE) (BASE).end

struct	NullValidate 
{ 
	void operator()(const void* elem) { } 
};

/*遍历(由start -> end)list中所有的单元，并执行functor函数*/
template <typename List, class Functor>
void ut_list_map(List& list, ut_list_node<typename List::elem_type> List::elem_type::*node, Functor functor)
{
	ulint count = 0;
	for(typename List::elem_type* elem = list.start; elem != NULL; elem = (elem->*node).next, ++count){
		functor(elem);
	}

	ut_a(count == list.count);
}
/*由 end -> start*/
template <typename List, class Functor>
void ut_list_validate(List&	list, ut_list_node<typename List::elem_type> List::elem_type::*node, Functor functor = NullValidate())
{
	ut_list_map(list, node, functor);

	ulint count = 0;
	for (typename List::elem_type* elem = list.end; elem != 0; elem = (elem->*node).prev, ++count){
			functor(elem);
	}

	ut_a(count == list.count);
}

#define UT_LIST_VALIDATE(NAME, TYPE, LIST, FUNCTOR)	ut_list_validate(LIST, &TYPE::NAME, FUNCTOR)

/*检查连续性*/
#define UT_LIST_CHECK(NAME, TYPE, LIST)	ut_list_validate(LIST, &TYPE::NAME, NullValidate())

#endif

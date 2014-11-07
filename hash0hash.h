#ifndef __HASH_0_HASH_H_
#define __HASH_0_HASH_H_

#include "univ.h"
#include "mem0mem.h"

#ifndef UNIV_HOTBACKUP
#include "sync0sync.h"
#include "sync0rw.h"
#endif

#define hash_create hash0_create

/*这个枚举会指定hash_table_t中的sync_obj*/
enum hash_table_sync_t
{
	HASH_TABLE_SYNC_NONE = 0, /*无锁类型*/
	HASH_TABLE_SYNC_MUTEX,	  /*用互斥锁访问HASH TABLE*/
	HASH_TABLE_SYNC_RW_LOCK,  /*用读写锁访问HASH TABLE*/
};

struct hash_cell_t
{
	void* node;
};

#define HASH_TABLE_MAGIC_N	76561114

struct hash_table_t
{
	enum hash_table_sync_t	type;		/*hash table的同步类型*/
	ulint					n_cells;	/*hash桶个数*/
	hash_cell_t*			array;		/*hash桶数组*/

#ifndef UNIV_HOTBACKUP
	ulint					n_sync_obj;
	union{ /*同步锁*/
		ib_mutex_t*			mutexes;
		rw_lock_t*			rw_locks;
	}sync_obj;
	/*heaps的单元个数和n_sync_obj一样*/
	mem_heap_t**			heaps;
#endif
	mem_heap_t*				heap;

	ulint					magic_n;	/*校验魔法字*/
#endif
};

UNIV_INTERN hash_table_t* hash_create(ulint n);

UNIV_INTERN void hash_create_sync_obj_func(hash_table_t* table, enum hash_table_sync_t type, ulint n_sync_obj);
/*创建hash table*/
#define hash_create_sync_obj(t, s, n, level) hash_create_sync_obj_func(t, s, n)

UNIV_INTERN void hash_table_free(hash_table_t* table);

UNIV_INTERN ulint hash_calc_hash(ulint fold, hash_table_t* table);

UNIV_INLINE hash_cell_t* hash_get_nth_cell(hash_table_t* table, ulint n);

UNIV_INLINE void hash_table_clear(hash_table_t* table);

UNIV_INLINE ulint hash_get_n_cells(hash_table_t* table);

UNIV_INLINE ulint hash_get_sync_obj_index(hash_table_t* table, ulint fold);

UNIV_INLINE mem_heap_t* hash_get_nth_heap(hash_table_t* table, ulint i);

UNIV_INLINE mem_heap_t* hash_get_heap(hash_table_t* table, ulint fold);

UNIV_INLINE ib_mutex_t* hash_get_nth_mutex(hash_table_t* table, ulint i);

UNIV_INLINE rw_lock_t* hash_get_nth_lock(hash_table_t* table, ulint i);

UNIV_INLINE ib_mutex_t* hash_get_mutex(hash_table_t* table, ulint fold);

UNIV_INLINE rw_lock_t* hash_get_lock(hash_table_t* table, ulint fold);

UNIV_INTERN void hash_mutex_enter(hash_table_t* table, ulint fold);

UNIV_INTERN void hash_mutex_exit(hash_table_t* table, ulint fold);

UNIV_INTERN void hash_mutex_enter_all(hash_table_t* table);

UNIV_INTERN void hash_mutex_exit_all(hash_table_t* table);
/*解除除keep_mutex以外的所有锁*/
UNIV_INTERN void hash_mutex_exit_all_but(hash_table_t* table, ib_mutex_t* keep_mutex);

UNIV_INTERN void hash_lock_s(hash_table_t* table, ulint fold);

UNIV_INTERN void hash_unlock_s(hash_table_t* table, ulint fold);

UNIV_INTERN void hash_lock_x(hash_table_t* table, ulint fold);

UNIV_INTERN void hash_unlock_x(hash_table_t* table, ulint fold);

UNIV_INTERN void hash_lock_x_all(hash_table_t* table);

UNIV_INTERN void hash_unlock_x_all(hash_table_t* table);
/*解除除keep_lock以外的所有锁*/
UNIV_INTERN void hash_unlock_x_all_put(hash_table_t* table, rw_lock_t* keep_lock);


#ifndef UNIV_HOTBACKUP
#define HASH_ASSERT_OWN(TABLE, FOLD) \
	ut_ad((TABLE)->type != HASH_TABLE_SYNC_MUTEX || (mutex_own(hash_get_mutex((TABLE), FOLD))));
#else
#define HASH_ASSERT_OWN(TABLE, FOLD)
#endif

/*向hash table插入一个（fold, data）*/
#define HASH_INSERT(TYPE, NAME, TABLE, FOLD, DATA) \
do{ \
	hash_cell_t*		cell3333; \
	TYPE*				struct333; \
	(DATA)->NAME_BUFFER = NULL; \
	cell3333 = hash_get_nth_cell(TABLE, hash_calc_hash(FOLD, TABLE)); \
	if(cell3333->node == NULL) \
		cell3333->node = DATA; \
	else{ \
		struct333 = (TYPE*)cell3333->node; \
		while(struct333->NAME != NULL) \
			struct333 = (TYPE*)struct333->NAME; \
		struct333->NAME = DATA; \
	} \
}while(0)

# define HASH_ASSERT_VALID(DATA) do {} while (0)
# define HASH_INVALIDATE(DATA, NAME) do {} while (0)

#define HASH_DELETE(TYPE, NAME, TABLE, FOLD, DATA) \
do{ \
	hash_cell_t*	cell3333; \
	TYPE*			struct3333; \
	HASH_ASSERT_OWN(TABLE, FOLD); \
	cell3333 = hash_get_nth_cell(TABLE, hash_calc_hash(FOLD, TABLE)); \
	if(cell3333->node == DATA){ \
		HASH_ASSERT_VALID(DATA->NAME); \
		cell3333->node = DATA->NAME; \
	} \
	else{ \
		struct3333 = (TYPE*)cell3333->node; \
		while(struct3333->NAME != DATA){ \
			struct3333 = (TYPE*)struct3333->NAME; \
			ut_a(struct3333); \
		} \
		struct3333->NAME = DATA->NAME; \
	} \
	HASH_INVALIDATE(DATA, NAME); \
}while(0)

/*获得cell的第一个单元*/
#define HASH_GET_FIRST(TABLE, HASH_VAL) (hash_get_nth_cell(TABLE, HASH_VAL)->NODE)
/*获得data的下一个单元*/
#define HASH_GET_NEXT(NAME, DATA) ((DATA)->NAME)

#define HASH_SEACH(NAME, TABLE, FOLD, TYPE, DATA, ASSERTION, TEST) \
{ \
	HASH_ASSERT_OWN(TABLE, FOLD); \
	(DATA) = (TYPE)HASH_GET_FIRST(TABLE, hash_calc_hash(FOLD, TABLE)); \
	HASH_ASSERT_VALID(DATA); \
	while((DATA) != NULL){ \
		ASSERTION; \
		if(TEST) break; \
		else{ \
			HASH_ASSERT_VALID(HASH_GET_NEXT(NAME, DATA)); \
			(DATA) = (TYPE) HASH_GET_NEXT(NAME, DATA); \
		} \
	} \
}

#define HASH_SEARCH_ALL(NAME, TABLE, TYPE, DATA, ASSERTION, TEST)	\
do{ \
	ulint	i3333; \
	for (i3333 = (TABLE)->n_cells; i3333--; ) {	\
	(DATA) = (TYPE) HASH_GET_FIRST(TABLE, i3333); \
	while ((DATA) != NULL) { \
		HASH_ASSERT_VALID(DATA); \
		ASSERTION; \
		if (TEST) break; \
		(DATA) = (TYPE) HASH_GET_NEXT(NAME, DATA); \
	} \
	if ((DATA) != NULL)	break;	\
}while(0)

#define HASH_DELETE_AND_COMPACT(TYPE, NAME, TABLE, NODE) \
do{ \
	TYPE*		node111; \
	TYPE*		top_node111; \
	hash_cell_t* cell111; \
	ulint		fold111; \
	\
	fold111 = (NODE)->fold; \
	HASH_DELETE(TYPE, NAME, TABLE, fold111, NODE);   \ /*删除掉需要删除的node*/
	\
	top_node111 = (TYPE*)mem_heap_get_top(hash_get_heap(TABLE, fold111), sizoef(TYPE)); \/*获得heap top的地址，如果这个地址存储的内容不是NODE的内容，将这个问题位置的内容和NODE置换，并释放掉top的空间，保持空间的连续性*/
	\
	if(top_node111 != (NODE)){  \
		*(NODE) = *top_node111; \
		cell111 = hash_get_nth_cell(TABLE, hash_calc_hash(fold111, TABLE)); \
		if(cell111->node == top_node111){ \
			cell111->node = NODE; \
		} \
		else{ \
			node111 = cell111->node; \
			while(top_node111 != HASH_GET_NEXT(NAME, node111)){ \
				node111 = HASH_GET_NEXT(NAME, node111); \
			} \
			node111->NAME = NODE; \
		} \
	} \
	mem_heap_free_top(hash_get_heap(TABLE, fold111), sizeof(TYPE)); \ /*释放掉heap top指向的空间(大小为TYPE结构的内存大小)*/
}while(0)

#define HASH_MIGRAGTE(OLD_TABLE, NEW_TABLE, NODE_TYPE, PTR_NAME, FOLD_FUNC) \
do{ \
	ulint i2222; \
	ulint cell_count2222; \
	cell_count2222 = hash_get_n_cells(OLD_TABLE); \
	\
	for(i2222 = 0; i2222 < cell_count2222; i2222 ++){ \
		NODE_TYPE* node2222 = HASH_GET_FIRST(TABLE, i2222); \
		while(node2222 != NULL){ \
			NODE_TYPE* next2222 = node2222->PTR_NAME; \
			ulint fold2222 = FOLD_FUNC(node2222);  \
			HASH_INSERT(NODE_TYPE, PTR_NAME, NEW_TABLE, fold2222, node2222); \
			node2222 = next2222; \
		} \
	} \
}while(0)

#endif

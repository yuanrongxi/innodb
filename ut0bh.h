#ifndef __INNOBASE_UT0BH_H
#define __INNOBASE_UT0BH_H

/*这个文件实现的是二进制最小堆排序的数据结构*/

#include "univ.h"

/*最小堆的比较函数指针*/
typedef int (*ib_bh_cmp_t)(const void* p1, const void* p2);

struct ib_bh_t
{
	ulint	max_elems;
	ulint	n_elems;
	ulint	sizeof_elem;
	ib_bh_cmp_t compare;
};

UNIV_INLINE ulint ib_bh_size(const ib_bh_t* ib_bh);

UNIV_INLINE ibool ib_bh_is_empty(const ib_bh_t* ib_bh);

UNIV_INLINE ibool ib_bh_is_full(const ib_bh_t* ib_bh);

UNIV_INLINE void* ib_bh_get(ib_bh_t* ib_bh, ulint i);

UNIV_INLINE void* ib_bh_set(ib_bh_t* ib_bh, ulint i, const void* elem);

UNIV_INLINE void* ib_bh_first(ib_bh_t* ib_bh);

UNIV_INLINE void* ib_bh_last(ib_bh_t* ib_bh);

UNIV_INTERN ib_bh_t* ib_bh_create(ib_bh_cmp_t compare, ulint sizeof_elem, ulint max_elems);

UNIV_INTERN void ib_bh_free(ib_bh_t* ib_bh);

UNIV_INTERN void* ib_bh_push(ib_bh_t* ib_bh, const void* elem);

UNIV_INTERN void ib_bh_pop(ib_bh_t* ib_bh);

#endif

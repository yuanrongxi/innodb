#include "ut0byte.h"

UNIV_INLINE ib_uint64_t ut_ull_create(ulint high, ulint low)
{
	ut_ad(high <= ULINT32_MASK);
	ut_ad(low <= ULINT32_MASK);

	return (((ib_uint64_t)high) << 32 | low);
}

UNIV_INLINE ib_uint64_t ut_uint64_algin_down(ib_uint64_t n, ulint align_no)
{
	ut_ad(align_no);
	ut_ad(ut_is_2pow(align_no));

	return (n & ~((ib_uint64_t)align_no - 1));
}

UNIV_INLINE ib_uint64_t ut_uint64_algin_up(ib_uint64_t n, ulint algin_no)
{
	ib_uint64_t align_1 = (ib_uint64_t)algin_no - 1;

	ut_ad(align_no > 0);
	ut_ad(ut_is_2pow(align_no));

	return ((n + align_1) & ~align_1);
}

UNIV_INLINE void* ut_align(const void* ptr, ulint align_no)
{
	ut_ad(align_no > 0);
	ut_ad(((align_no - 1) & align_no) == 0);
	ut_ad(ptr);

	ut_ad(sizeof(void*) == sizeof(ulint));

	return ((void*)((((ulint)ptr) + align_no - 1) & (align_no - 1)));
}

UNIV_INLINE void* ut_align_down(const void* ptr, ulint align_no)
{
	ut_ad(align_no > 0);
	ut_ad(((align_no - 1) & align_no) == 0);
	ut_ad(ptr);

	ut_ad(sizeof(void*) == sizeof(ulint));

	return ((void *)(((ulint)ptr) & ~(align_no - 1)));
}

UNIV_INLINE ulint ut_align_offset(const void* ptr, ulint align_no)
{
	ut_ad(align_no > 0);
	ut_ad(((align_no - 1) & align_no) == 0);
	ut_ad(ptr);

	ut_ad(sizeof(void*) == sizeof(ulint));

	return (((ulint) ptr) & (align_no - 1));
}
/*判断a的第n位上是否为1*/
UNIV_INLINE ibool ut_bit_get_nth(ulint a, ulint n)
{
	ut_ad(n < 8 * sizeof(ulint));

#if TRUE != 1
#error "TRUE != 1"
#endif
	return (1 & (a >> n));
}

/*将a的第n位设置为0 或者1*/
UNIV_INLINE ulint ut_bit_set_nth(ulint a, ulint n, ibool val)
{
	ut_ad(n < 8 * sizeof(ulint));

	if (val) {
		return((1 << n) | a);
	} else {
		return(~(1 << n) & a);
	}
}


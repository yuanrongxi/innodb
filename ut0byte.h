#ifndef ut0byte_h
#define ut0byte_h

/*基础的常量和mySQL相关的常量*/
#include "univ.h"

/*构建一个64位的整型数*/
UNIV_INLINE ib_uint64_t ut_ull_create(ulint high, ulint low) __attribute__((const));

/*小于n的最大algin_no的倍数，algin_no必须是2的次方数，例如ut_uint64_algin_down（11，2） = 10*/
UNIV_INLINE ib_uint64_t ut_uint64_algin_down(ib_uint64_t n, ulint algin_no);

/*大于n的最小algin_no的倍数，algin_no必须是2的次方数，例如ut_uint64_algin_down（11，2） = 12*/
UNIV_INLINE ib_uint64_t ut_uint64_algin_up(ib_uint64_t n, ulint algin_no);

/*地址对齐，和ut_uint64_algin_up类似*/
UNIV_INLINE void* ut_align(const void* ptr, ulint align_no);

/*地址对齐，和ut_uint64_algin_down类似*/
UNIV_INLINE void* ut_align_down(const void* ptr, ulint align_no) __attribute__(const);

/*获得align_no为单位地址偏移量*/
UNIV_INLINE ulint ut_align_offset(const void* ptr, ulint align_no) __attribute__(const);

/*判断a的第n位上是否为1*/
UNIV_INLINE ibool ut_bit_get_nth(ulint a, ulint n);

/*将a的第n位设置为0 或者1*/
UNIV_INLINE ulint ut_bit_set_nth(ulint a, ulint b, ibool val);
#endif






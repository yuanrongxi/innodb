#include "ut0rnd.h"

#define UT_HASH_RANDOM_MASK	1463735687
#define UT_HASH_RANDOM_MASK2	1653893711

#ifndef UNIV_INNOCHECKSUM

#define UT_RND1			151117737
#define UT_RND2			119785373
#define UT_RND3			 85689495
#define UT_RND4			 76595339
#define UT_SUM_RND2		 98781234
#define UT_SUM_RND3		126792457
#define UT_SUM_RND4		 63498502
#define UT_XOR_RND1		187678878
#define UT_XOR_RND2		143537923

/** Seed value of ut_rnd_gen_ulint() */
extern	ulint	 ut_rnd_ulint_counter;

/********************************************************/

UNIV_INLINE
	void
	ut_rnd_set_seed(
	/*============*/
	ulint	 seed)		 /*!< in: seed */
{
	ut_rnd_ulint_counter = seed;
}

UNIV_INLINE
	ulint
	ut_rnd_gen_next_ulint(
	/*==================*/
	ulint	rnd)	/*!< in: the previous random number value */
{
	ulint	n_bits;

	n_bits = 8 * sizeof(ulint);

	rnd = UT_RND2 * rnd + UT_SUM_RND3;
	rnd = UT_XOR_RND1 ^ rnd;
	rnd = (rnd << 20) + (rnd >> (n_bits - 20));
	rnd = UT_RND3 * rnd + UT_SUM_RND4;
	rnd = UT_XOR_RND2 ^ rnd;
	rnd = (rnd << 20) + (rnd >> (n_bits - 20));
	rnd = UT_RND1 * rnd + UT_SUM_RND2;

	return(rnd);
}

UNIV_INLINE
	ulint
	ut_rnd_gen_ulint(void)
	/*==================*/
{
	ulint	rnd;

	ut_rnd_ulint_counter = UT_RND1 * ut_rnd_ulint_counter + UT_RND2;

	rnd = ut_rnd_gen_next_ulint(ut_rnd_ulint_counter);

	return(rnd);
}

UNIV_INLINE
	ulint
	ut_rnd_interval(
	/*============*/
	ulint	low,	/*!< in: low limit; can generate also this value */
	ulint	high)	/*!< in: high limit; can generate also this value */
{
	ulint	rnd;

	ut_ad(high >= low);

	if (low == high) {

		return(low);
	}

	rnd = ut_rnd_gen_ulint();

	return(low + (rnd % (high - low)));
}

UNIV_INLINE
	ibool
	ut_rnd_gen_ibool(void)
	/*=================*/
{
	ulint	 x;

	x = ut_rnd_gen_ulint();

	if (((x >> 20) + (x >> 15)) & 1) {

		return(TRUE);
	}

	return(FALSE);
}

UNIV_INLINE
	ulint
	ut_fold_ull(
	/*========*/
	ib_uint64_t	d)	/*!< in: 64-bit integer */
{
	return(ut_fold_ulint_pair((ulint) d & ULINT32_MASK,
		(ulint) (d >> 32)));
}

UNIV_INLINE
	ulint
	ut_fold_string(
	/*===========*/
	const char*	str)	/*!< in: null-terminated string */
{
	ulint	fold = 0;

	ut_ad(str);

	while (*str != '\0') {
		fold = ut_fold_ulint_pair(fold, (ulint)(*str));
		str++;
	}

	return(fold);
}

#endif /* !UNIV_INNOCHECKSUM */

UNIV_INLINE
	ulint
	ut_fold_ulint_pair(
	/*===============*/
	ulint	n1,	/*!< in: ulint */
	ulint	n2)	/*!< in: ulint */
{
	return(((((n1 ^ n2 ^ UT_HASH_RANDOM_MASK2) << 8) + n1)
		^ UT_HASH_RANDOM_MASK) + n2);
}

UNIV_INLINE
	ulint
	ut_fold_binary(
	/*===========*/
	const byte*	str,	/*!< in: string of bytes */
	ulint		len)	/*!< in: length */
{
	ulint		fold = 0;
	const byte*	str_end	= str + (len & 0xFFFFFFF8);

	ut_ad(str || !len);

	while (str < str_end) {
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
	}

	switch (len & 0x7) {
	case 7:
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
	case 6:
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
	case 5:
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
	case 4:
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
	case 3:
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
	case 2:
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
	case 1:
		fold = ut_fold_ulint_pair(fold, (ulint)(*str++));
	}

	return(fold);
}

/** These random numbers are used in ut_find_prime */
/*@{*/
#define	UT_RANDOM_1	1.0412321
#define	UT_RANDOM_2	1.1131347
#define UT_RANDOM_3	1.0132677
/*@}*/

/** Seed value of ut_rnd_gen_ulint(). */
UNIV_INTERN ulint ut_rnd_ulint_counter = 65654363;

UNIV_INTERN ulint ut_find_prime(ulint n)
{
	ulint pow2;
	ulint i;

	n += 100;

	pow2 = 1;
	while(pow2 * 2 < n)
		pow2 = pow2 * 2;

	if((double) n < 1.05 * (double) pow2)
		n = (ulint)((double)n * UT_RANDOM_1);

	pow2 = pow2 * 2;
	if((double) n > 0.95 * (double) pow2)
		n = (ulint)((double) n * UT_RANDOM_2);

	if(n > pow2 - 20)
		n += 30;

	n = (ulint)((double)n * UT_RANDOM_3);
	for(;; n ++){
		i = 2; 
		while(i * i <= n){
			if(n % i == 0)
				goto next_n;

			i ++;
		}

		break;
next_n:
	}

	return n;
}





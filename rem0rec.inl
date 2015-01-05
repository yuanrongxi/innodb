#include "mach0data.h"
#include "ut0byte.h"

/* Offsets of the bit-fields in the record. NOTE! In the table the most 
significant bytes and bits are written below less significant.

	(1) byte offset		(2) bit usage within byte
	downward from
	origin ->	1	8 bits pointer to next record
				2	8 bits pointer to next record
				3  	1 bit short flag
					7 bits number of fields
				4	3 bits number of fields
					5 bits heap number
				5	8 bits heap number
				6	4 bits n_owned
					4 bits info bits
*************************************************************************/

#define REC_NEXT			2
#define REC_NEXT_MASK		0xFFFF
#define REC_NEXT_SHIFT		0

#define REC_SHORT			3		/* This is single byte bit-field */
#define	REC_SHORT_MASK		0x1
#define REC_SHORT_SHIFT		0

#define	REC_N_FIELDS		4
#define REC_N_FIELDS_MASK	0x7FE
#define	REC_N_FIELDS_SHIFT	1

#define	REC_HEAP_NO			5
#define REC_HEAP_NO_MASK	0xFFF8
#define	REC_HEAP_NO_SHIFT	3

#define REC_N_OWNED			6	/* This is single byte bit-field */
#define	REC_N_OWNED_MASK	0xF
#define REC_N_OWNED_SHIFT	0

#define	REC_INFO_BITS_MASK	0xF0
#define REC_INFO_BITS_SHIFT	0

#define REC_INFO_DELETED_FLAG 	0x20

#define REC_1BYTE_SQL_NULL_MASK	0x80
#define REC_2BYTE_SQL_NULL_MASK	0x8000

#define REC_2BYTE_EXTERN_MASK	0x4000


void rec_set_nth_field_null_bit(rec_t* rec, ulint i, ibool val);
void rec_set_nth_field_sql_null(rec_t* rec, ulint n);

/*获取一个头域*/
UNIV_INLINE ulint rec_get_bit_field_1(rec_t* rec, ulint offs, ulint mask, ulint shift)
{
	ut_ad(rec);
	return ((mach_read_from_1(rec - offs) & mask) >> shift);
}

/*设置一个头域*/
UNIV_INLINE void rec_set_bit_field_1(rec_t* rec, ulint val, ulint offs, ulint mask, ulint shift)
{
	ut_ad(rec);
	ut_ad(offs <= REC_N_EXTRA_BYTES);
	ut_ad(mask);
	ut_ad(mask <= 0xFF);
	ut_ad(((mask >> shift) << shift) == mask);
	ut_ad(((val << shift) & mask) == (val << shift));

	mach_write_to_1(rec - offs, (mach_read_from_1(rec - offs) & ~mask) | (val << shift));
}

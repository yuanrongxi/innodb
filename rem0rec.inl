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

	mach_write_to_1(rec - offs, (mach_read_from_1(rec - offs) & ~mask) | (val >> shift));
}

UNIV_INLINE ulint rec_get_bit_field_2(rec_t* rec, ulint offs, ulint mask, ulint shift)
{
	ut_ad(rec);
	return ((mach_read_from_2(rec - offs) & mask) >> shift);
}

UNIV_INLINE void rec_set_bit_field_2(rec_t* rec, ulint val, ulint offs, ulint mask, ulint shift)
{
	ut_ad(rec);
	ut_ad(offs <= REC_N_EXTRA_BYTES);
	ut_ad(mask > 0xFF);
	ut_ad(mask <= 0xFFFF);
	ut_ad((mask >> shift) & 1);
	ut_ad(0 == ((mask >> shift) & ((mask >> shift) + 1)));
	ut_ad(((mask >> shift) << shift) == mask);
	ut_ad(((val << shift) & mask) == (val << shift));

	mach_write_to_2(rec - offs, (mach_read_from_2(rec - offs) & mask) | (val << shift));
}

UNIV_INLINE ulint rec_get_next_offs(rec_t* rec)
{
	ulint ret;
	
	ut_ad(rec);
	ret = rec_get_bit_field_2(rec, REC_NEXT, REC_NEXT_MASK, REC_NEXT_SHIFT);
	ut_ad(ret < UNIV_PAGE_SIZE);

	return ret;
}

UNIV_INLINE void rec_set_next_offs(rec_t* rec, ulint next)
{
	ut_ad(rec);
	ut_ad(UNIV_PAGE_SIZE > next);

	rec_set_bit_field_2(rec, next, REC_NEXT, REC_NEXT_MASK, REC_NEXT_SHIFT);
}

UNIV_INLINE ulint rec_get_n_fields(rec_t* rec)
{
	ulint ret;
	ut_ad(rec);

	ret = rec_get_bit_field_2(rec, REC_N_FIELDS, REC_N_FIELDS_MASK, REC_N_FIELDS_SHIFT);
	ut_ad(ret <= REC_MAX_N_FIELDS);
	ut_ad(ret > 0);

	return ret;
}

UNIV_INLINE void rec_set_n_fields(rec_t* rec, ulint n_fields)
{
	ut_ad(rec);
	ut_ad(n_fields <= REC_MAX_N_FIELDS);
	ut_ad(n_fields > 0);

	rec_set_bit_field_2(rec, n_fields, REC_N_FIELDS, REC_N_FIELDS_MASK, REC_N_FIELDS_SHIFT);
}

UNIV_INLINE ulint rec_get_n_owned(rec_t* rec)
{
	ulint ret;
	ut_ad(rec);

	ret = rec_get_bit_field_1(rec, REC_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
	ut_ad(ret <= REC_MAX_N_OWNED);

	return ret;
}

UNIV_INLINE void rec_set_n_owned(rec_t* rec, ulint n_owned)
{
	ut_ad(rec);
	ut_ad(n_owned <= REC_MAX_N_OWNED);

	rec_set_bit_field_1(rec, n_owned, REC_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

UNIV_INLINE ulint rec_get_info_bits(rec_t* rec)
{
	ulint ret;
	ut_ad(rec);

	ret = rec_get_bit_field_1(rec, REC_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
	return ret;
}

UNIV_INLINE void rec_set_info_bits(rec_t* rec, ulint bits)
{
	ut_ad(rec);
	ut_ad((bits & ~REC_INFO_BITS_MASK) == 0);

	rec_set_bit_field_1(rec, bits, REC_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

UNIV_INLINE ibool rec_info_bits_get_deleted_flag(ulint info_bits)
{
	if(info_bits & REC_INFO_DELETED_FLAG)
		return TRUE;

	return FALSE;
}

UNIV_INLINE void rec_set_deleted_flag(rec_t* rec, ibool flag)
{
	ulint	old_val;
	ulint	new_val;

	ut_ad(TRUE == 1);
	ut_ad(flag <= TRUE);

	old_val = rec_get_info_bits(rec);
	if(flag)
		new_val = REC_INFO_DELETED_FLAG | old_val;
	else
		new_val = ~REC_INFO_DELETED_FLAG & old_val;

	rec_set_info_bits(rec, new_val);
}

UNIV_INLINE ulint rec_get_heap_no(rec_t* rec)
{
	ulint ret;

	ut_ad(rec);
	ret = rec_get_bit_field_2(rec, REC_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
	ut_ad(ret <= REC_MAX_HEAP_NO);

	return ret;
}

UNIV_INLINE void  rec_set_heap_no(rec_t* rec, ulint heap_no)
{
	ut_ad(heap_no <= REC_MAX_HEAP_NO);
	rec_set_bit_field_2(rec, heap_no, REC_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

UNIV_INLINE ibool rec_get_1byte_offs_flag(rec_t* rec)
{
	return(rec_get_bit_field_1(rec, REC_SHORT, REC_SHORT_MASK, REC_SHORT_SHIFT));
}

UNIV_INLINE void rec_set_1byte_offs_flag(rec_t* rec, ibool flag)
{
	rec_set_bit_field_1(rec, flag, REC_SHORT, REC_SHORT_MASK, REC_SHORT_SHIFT);
}

UNIV_INLINE ulint rec_1_get_field_end_info(rec_t* rec, ulint n)
{
	ut_ad(rec_get_1byte_offs_flag(rec));
	ut_ad(n < rec_get_n_fields(rec));

	return (mach_read_from_1(rec - (REC_N_EXTRA_BYTES + n + 1)));
}

UNIV_INLINE ulint rec_2_get_field_end_info(rec_t* rec, ulint n)
{
	ut_ad(!rec_get_1byte_offs_flag(rec));
	ut_ad(n < rec_get_n_fields(rec));

	return (mach_read_from_2(rec - (REC_N_EXTRA_BYTES + 2 * n + 2)));
}

UNIV_INLINE ibool rec_get_nth_field_extern_bit(rec_t* rec, ulint i)
{
	ulint info;

	if(rec_get_1byte_offs_flag(rec))
		return FALSE;

	info = rec_2_get_field_end_info(rec, i);
	if(info & REC_2BYTE_EXTERN_MASK)
		return TRUE;

	return FALSE;
}

UNIV_INLINE ibool rec_contains_externally_stored_field(rec_t* rec)
{
	ulint	n;
	ulint	i;

	if(rec_get_1byte_offs_flag(rec))
		return FALSE;

	n = rec_get_n_fields(rec);
	for(i = 0; i < n; i ++){
		if(rec_get_nth_field_extern_bit(rec, i))
			return TRUE;
	}

	return FALSE;
}

UNIV_INLINE ulint rec_1_get_prev_field_end_info(rec_t* rec, ulint n)
{
	ut_ad(rec_get_1byte_offs_flag(rec));
	ut_ad(n <= rec_get_n_fields(rec));

	return (mach_read_from_1(rec - (REC_N_EXTRA_BYTES + n)));
}

UNIV_INLINE ulint rec_2_get_prev_field_end_info(rec_t* rec, ulint n)
{
	ut_ad(!rec_get_1byte_offs_flag(rec));
	ut_ad(n <= rec_get_n_fields(rec));

	return (mach_read_from_2(rec - (REC_N_EXTRA_BYTES + 2 * n)));
}

UNIV_INLINE void rec_1_set_field_end_info(rec_t* rec, ulint n, ulint info)
{
	ut_ad(rec_get_1byte_offs_flag(rec));
	ut_ad(n < rec_get_n_fields(rec));

	mach_write_to_1(rec - (REC_N_EXTRA_BYTES + n + 1), info);
}

UNIV_INLINE void rec_2_set_field_end_info(rec_t* rec, ulint n, ulint info)
{
	ut_ad(!rec_get_1byte_offs_flag(rec));
	ut_ad(n < rec_get_n_fields(rec));

	mach_write_to_2(rec - (REC_N_EXTRA_BYTES + 2 * n + 2), info);
}

UNIV_INLINE ulint rec_1_get_field_start_offs(rec_t* rec, ulint n)
{
	ut_ad(rec_get_1byte_offs_flag(rec));
	ut_ad(n <= rec_get_n_fields(rec));

	if(n == 0)
		return 0;

	return (rec_1_get_prev_field_end_info(rec, n) & ~REC_1BYTE_SQL_NULL_MASK);
}

UNIV_INLINE ulint rec_2_get_field_start_offs(rec_t* rec, ulint n)
{
	ut_ad(!rec_get_1byte_offs_flag(rec));
	ut_ad(n <= rec_get_n_fields(rec));

	if(n == 0)
		return 0;

	return (rec_2_get_prev_field_end_info(rec, n) & ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK));
}

UNIV_INLINE ulint rec_get_field_start_offs(rec_t* rec, ulint n)
{
	ut_ad(rec);
	ut_ad(n <= rec_get_n_fields(rec));
	
	if(n == 0)
		return 0;

	if(rec_get_1byte_offs_flag(rec))
		return rec_1_get_field_start_offs(rec, n);
	else
		return rec_2_get_field_start_offs(rec, n);
}

/*获得一个列的占用的空间数*/
UNIV_INLINE ulint rec_get_nth_field_size(rec_t* rec, ulint n)
{
	ulint	os;
	ulint	next_os;

	os = rec_get_field_start_offs(rec, n);
	next_os = rec_get_field_start_offs(rec, n + 1);

	ut_ad(next_os - os < UNIV_PAGE_SIZE);

	return next_os - os;
}

UNIV_INLINE void rec_copy_nth_field(void* buf, rec_t* rec, ulint n, ulint* len)
{
	byte* ptr;
	ut_ad(buf && rec && len);

	ptr = rec_get_nth_field(rec, n, len);
	if(*len == UNIV_SQL_NULL)
		return;

	ut_memcpy(buf, ptr, *len);
}

UNIV_INLINE void rec_set_nth_field(rec_t* rec, ulint n, void* data, ulint len)
{
	byte*	data2;
	ulint	len2;

	ut_ad((len == UNIV_SQL_NULL) || (rec_get_nth_field_size(rec, n) == len));

	if(len == UNIV_SQL_NULL){
		rec_set_nth_field_sql_null(rec, n);
		return ;
	}

	data2 = rec_get_nth_field(rec, n, &len2);
	ut_memcpy(data2, data, len);
	if(len2 == UNIV_SQL_NULL)
		rec_set_nth_field_null_bit(rec, n, FALSE);
}

UNIV_INLINE ulint rec_get_data_size(rec_t* rec)
{
	ut_ad(rec);
	return rec_get_field_start_offs(rec, rec_get_n_fields(rec));
}

UNIV_INLINE ulint rec_get_extra_size(rec_t* rec)
{
	ulint n_fields;
	ut_ad(rec);

	n_fields = rec_get_n_fields(rec);
	if(rec_get_1byte_offs_flag(rec))
		return (REC_N_EXTRA_BYTES + n_fields);
	else
		return (REC_N_EXTRA_BYTES + 2 * n_fields);
}

/*返回物理记录的长度*/
UNIV_INLINE ulint rec_get_size(rec_t* rec)
{
	ulint n_fields;
	ut_ad(rec);

	n_fields = rec_get_n_fields(rec);
	if(rec_get_1byte_offs_flag(rec))
		return (REC_N_EXTRA_BYTES + n_fields + rec_1_get_field_start_offs(rec, n_fields));
	else
		return (REC_N_EXTRA_BYTES + 2 * n_fields + rec_2_get_field_start_offs(rec, n_fields));
}

/*获得记录末尾的位置*/
UNIV_INLINE byte* rec_get_end(rec_t* rec)
{
	return (rec + rec_get_data_size(rec));
}

/*获得记录起始位置*/
UNIV_INLINE byte* rec_get_start(rec_t* rec)
{
	return (rec - rec_get_extra_size(rec));
}

UNIV_INLINE rec_t* rec_copy(void* buf, rec_t* rec)
{
	ulint	extra_len;
	ulint	data_len;

	ut_ad(rec && buf);
	ut_ad(rec_validate(rec));

	extra_len = rec_get_extra_size(rec);
	data_len = rec_get_data_size(rec);

	ut_memcpy(buf, rec - extra_len, extra_len + data_len);
	/*指向列的开始位置,也就是rec_t指向的位置*/
	return (byte *)buf + extra_len;
}

UNIV_INLINE ulint rec_get_converted_extra_size(ulint data_size, ulint n_fields)
{
	if(data_size <= REC_1BYTE_OFFS_LIMIT)
		return REC_N_EXTRA_BYTES + n_fields;
	else
		return REC_N_EXTRA_BYTES + 2 * n_fields;
}

/*通过内存中的逻辑记录tuple获得实际物理记录的长度*/
UNIV_INLINE ulint rec_get_converted_size(dtuple_t* dtuple)
{
	ulint	data_size;
	ulint	extra_size;

	ut_ad(dtuple);
	ut_ad(dtuple_check_typed(dtuple));

	data_size = dtuple_get_data_size(dtuple);
	extra_size = rec_get_converted_extra_size(data_size, dtuple_get_n_fields(dtuple));

	return data_size + extra_size;
}

UNIV_INLINE ulint rec_fold(rec_t* rec, ulint n_fields, ulint n_bytes, dulint tree_id)
{
	ulint	i;
	byte*	data;
	ulint	len;
	ulint	fold;
	ulint	n_fields_rec;

	ut_ad(rec_validate(rec));
	ut_ad(n_fields <= rec_get_n_fields(rec));
	ut_ad((n_fields < rec_get_n_fields(rec)) || (n_bytes == 0));
	ut_ad(n_fields + n_bytes > 0);

	n_fields_rec = rec_get_n_fields(rec);
	if(n_fields > n_fields_rec)
		n_fields = n_fields_rec;

	if(n_fields == n_fields_rec)
		n_bytes = 0;

	fold = ut_fold_dulint(tree_id);
	for(i = 0; i < n_fields; i ++){
		data = rec_get_nth_field(rec, i, &len);

		if (len != UNIV_SQL_NULL)
			fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
	}

	if (n_bytes > 0) {/*列数过多，再加后面一列进行fold hash计算*/
		data = rec_get_nth_field(rec, i, &len);

		if (len != UNIV_SQL_NULL) {
			if (len > n_bytes)
				len = n_bytes;

			fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
		}
	}

	return fold;
}

/*将内存中的逻辑记录转成物理记录rec_t*/
UNIV_INLINE rec_t* rec_convert_dtuple_to_rec(byte* destination, dtuple_t* dtuple)
{
	return rec_convert_dtuple_to_rec_low(destination, dtuple, dtuple_get_data_size(dtuple));
}


#include "rem0cmp.h"
#include "srv0srv.h"

static int			cmp_debug_dtuple_rec_with_match(dtuple_t* dtuple, rec_t* rec, ulint matched_fields);

int					innobase_mysql_cmp(int mysql_type, unsigned char* a, unsigned int a_length, unsigned char* b, unsigned int b_length);


UNIV_INLINE ulint cmp_collate(dtype_t* type, ulint code)
{
	ut_ad(type->mtype == DATA_CHAR || type->mtype == DATA_VARCHAR);

	return (ulint)(srv_latin1_ordering[code]);
}

ibool cmp_types_are_equal(dtype_t* type1, dtype_t* type2)
{
	if((type1->mtype == DATA_VARCHAR && type2->mtype == DATA_CHAR)
		|| (type1->mtype == DATA_CHAR && type2->mtype == DATA_VARCHAR)
		|| (type1->mtype == DATA_FIXBINARY && type2->mtype == DATA_BINARY)
		|| (type1->mtype == DATA_BINARY && type2->mtype == DATA_FIXBINARY)
		|| (type1->mtype == DATA_MYSQL && type2->mtype == DATA_VARMYSQL)
		|| (type1->mtype == DATA_VARMYSQL && type2->mtype == DATA_MYSQL))
		return TRUE;

	if(type1->mtype != type2->mtype)
		return FALSE;

	if(type1->mtype == DATA_INT && type1->len != type2->len)
		return FALSE;

	return TRUE;
}

/*比较两个列的大小关系*/
static int cmp_whole_field(dtype_t* type, unsigned char* a, unsigned int a_length, unsigned char* b, unsigned int b_length)
{
	float		f_1;
	float		f_2;
	double		d_1;
	double		d_2;
	int			swap_flag = 1;
	ulint		data_type;

	data_type = type->mtype;
	switch(data_type){
	case DATA_DECIMAL:
		for(; a_length && *a == ' '; a++, a_length --);
		for(; b_length && *b == ' '; b++; b_length --);
		if(*a == '-'){
			if(*b == '-')
				return -1;

			a ++; 
			b ++;
			a_length --;
			b_length --;

			swap_flag = -1;
		}
		else if(*b == '-')
			return 1;

		while(a_length > 0 && (*a == '+' || *a == '0')){
			a ++; a_length --;
		}

		while(b_length > 0 && (*b == '+' || *b == '0')){
			b ++; b_length --;
		}

		if(a_length != b_length){
			if(a_length < b_length)
				return (-swap_flag);
			else 
				return swap_flag;
		}

		while(a_length > 0 && *a == *b){
			a ++;
			b ++;
			a_length --;
		}

		if(a_length == 0)
			return 0;

		if(*a > *b)
			return swap_flag;
		else
			return (-swap_flag);

		break;

	case DATA_FLOAT:
		f_1 = mach_float_read(a);
		f_2 = mach_float_read(b);
		if(f_1 > f_2)
			return 1;
		else if(f_1 < f_2)
			return -1;
		else 
			return 0;

	case DATA_VARMYSQL:
	case DATA_MYSQL:
		return innobase_mysql_cmp((int)(type->prtype & ~DATA_NOT_NULL), a, a_length, b, b_length);

	default:
		fprintf(stderr, "InnoDB: unknown type number %lu\n", data_type);
		ut_a(0);
	}

	return 0;
}

/*列数据比较*/
int cmp_data_data_slow(dtype_t* cur_type, byte* data1, ulint len1, byte* data2, ulint len2)
{
	ulint data1_byte, data2_byte;
	ulint cur_bytes;

	ut_ad(dtype_validate(cur_type));
	if(len1 == UNIV_SQL_NULL || len2 == UNIV_SQL_NULL){ /*其中一个数据为空*/
		if(len1 == len2)
			return 0;

		if(len1 == UNIV_SQL_NULL)
			return -1;

		return 1;
	}

	/*数规格类型数据，用cmp_whole_field进行比较*/
	if(cur_type->mtype >= DATA_FLOAT)
		return cmp_whole_field(cur_type, data1, len1, data2, len2);

	/*字串类型数据，按cmp_collate方式比较*/
	cur_bytes = 0;
	for(;;){
		if(len1 <= cur_bytes){ /*data1长度不够，用' '填充*/
			if(len2 <= cur_bytes)
				return 0;

			data1_byte = dtype_get_pad_char(cur_type);
			if(data1_byte == ULINT_UNDEFINED)
				return -1;
		}
		else
			data1_byte = *data1;

		if(len2 <= cur_bytes){
			data2_byte = dtype_get_pad_char(cur_type);
			if(data2_byte == ULINT_UNDEFINED)
				return 1;
		}
		else
			data2_byte = *data2;

		if(data1_byte == data2_byte) /*继续下一个字符*/
			goto next_byte;

		if (cur_type->mtype <= DATA_CHAR) {
			data1_byte = cmp_collate(cur_type, data1_byte);
			data2_byte = cmp_collate(cur_type, data2_byte);
		}

		if(data1_byte > data2_byte)
			return 1;
		else if(data1_byte < data2_byte)
			return -1;

next_byte:
		cur_bytes++;
		data1++;
		data2++;
	}

	return 0;
}

int cmp_dtuple_rec_with_match(dtuple_t* dtuple, rec_t* rec, ulint* matched_fields, ulint* matched_bytes)
{
	dtype_t*	cur_type;

	dfield_t*	dtuple_field;
	ulint		dtuple_f_len;

	byte*		dtuple_b_ptr;
	ulint		dtuple_byte;

	byte*		rec_b_ptr;
	ulint		rec_f_len;

	ulint		rec_byte;

	ulint		cur_field;
	ulint		cur_bytes;

	int			ret = 3333;

	ut_ad(dtuple && rec && matched_fields && matched_bytes);
	ut_ad(dtuple_check_typed(dtuple));

	cur_field = *matched_fields;
	cur_bytes = *matched_bytes;

	ut_ad(cur_field <= dtuple_get_n_fields_cmp(dtuple));
	ut_ad(cur_field <= rec_get_n_fields(rec));

	while(cur_field < dtuple_get_n_fields_cmp(dtuple)){
		/*获得对应tuple对应的列*/
		dtuple_field = dtuple_get_nth_field(dtuple, cur_field);
		cur_type = dfield_get_type(dtuple_field);

		/*获得rec_t对应的列*/
		dtuple_f_len = dfield_get_len(dtuple_field);
		rec_b_ptr = rec_get_nth_field(rec, cur_field, &rec_f_len);

		if(cur_bytes == 0){
			if(cur_field == 0){
				/*对bits做比较*/
				if(rec_get_info_bits(rec) & REC_INFO_MIN_REC_FLAG){
					if(dtuple_get_info_bits(dtuple) & REC_INFO_MIN_REC_FLAG)
						ret = 0;
					else
						ret = 1;

					goto order_resolved;
				}

				if(dtuple_get_info_bits(dtuple) & REC_INFO_MIN_REC_FLAG){ /*tuple是个最小记录*/
					ret = -1;
					goto order_resolved;
				}
			}

			if(rec_get_nth_field_extern_bit(rec, cur_field)){
				ret = 0;
				goto order_resolved;
			}

			if(dtuple_f_len == UNIV_SQL_NULL || rec_f_len == UNIV_SQL_NULL){
				if(dtuple_f_len == rec_f_len) /*都是空的field*/
					goto next_field;

				if(rec_f_len == UNIV_SQL_NULL)
					ret = 1;
				else
					ret = -1;

				goto order_resolved;
			}
		}

		if(cur_type->mtype >= DATA_FLOAT){ /*数值列比较*/
			ret = cmp_whole_field(cur_type, dfield_get_data(dtuple_field), dtuple_f_len, rec_b_ptr, rec_f_len);

			if(ret != 0){
				cur_bytes = 0;
				goto order_resolved;
			}
			else
				goto next_field;
		}

		/*字串列比较*/
		rec_b_ptr = rec_b_ptr + cur_bytes;
		dtuple_b_ptr = (byte*)dfield_get_data(dtuple_field) + cur_bytes;

		for(;;){
			if(rec_f_len <= cur_bytes){
				if(dtuple_f_len <= cur_bytes)
					goto next_field;

				rec_byte = dtype_get_pad_char(cur_type);
				if(rec_byte == ULINT_UNDEFINED){
					ret = 1;
					goto order_resolved;
				}
			}
			else
				rec_byte = *rec_b_ptr;

			if(dtuple_f_len <= cur_bytes){
				dtuple_byte = dtype_get_pad_char(cur_type);
				if(dtuple_byte == ULINT_UNDEFINED){
					ret = -1;
					goto order_resolved;
				}
			}
			else
				dtuple_byte = *dtuple_b_ptr;

			if(dtuple_byte == rec_byte)
				goto next_byte;

			if(cur_type->mtype <= DATA_CHAR){
				rec_byte = cmp_collate(cur_type, rec_byte);
				dtuple_byte = cmp_collate(cur_type, dtuple_byte);
			}

			if(dtuple_byte > rec_byte){
				ret = 1;
				goto order_resolved;
			}
			else if(dtuple_byte < rec_byte){
				ret = -1;
				goto order_resolved;
			}
next_byte:
			cur_bytes++;
			rec_b_ptr++;
			dtuple_b_ptr++;
		}
next_field:
		cur_field ++;
		cur_bytes = 0;

		ut_ad(cur_bytes == 0);
		ret = 0;
	}

order_resolved:
	ut_ad((ret >= - 1) && (ret <= 1));
	ut_ad(ret == cmp_debug_dtuple_rec_with_match(dtuple, rec,
		matched_fields));
	ut_ad(*matched_fields == cur_field); 

	*matched_fields = cur_field;	
	*matched_bytes = cur_bytes;

	return ret;
}

/*比较dtuple与物理记录rec之间的大小*/
int cmp_dtuple_rec(dtuple_t* dtuple, rec_t* rec)
{
	ulint matched_fields = 0;
	ulint matched_bytes = 0;

	return cmp_dtuple_rec_with_match(dtuple, rec, &matched_fields, &matched_bytes);
}

#include "rem0rec.h"
#include "mtr0mtr.h"
#include "mtr0log.h"


ulint rec_dummy;


byte* rec_get_nth_field(rec_t* rec, ulint n, ulint* len)
{
	ulint os, next_os;

	ut_ad(rec & len);
	ut_ad(n < rec_get_n_fields());

	if(n >= 1024){
		fprintf(stderr, "Error: trying to access field %lu in rec\n", n);
		ut_a(0);
	}

	if(rec == NULL){
		fprintf(stderr, "Error: rec is NULL \n");
		ut_a(0);
	}

	if(rec_get_1byte_offs_flag(rec)){
		os = rec_1_get_field_start_offs(rec, n);
		next_os = rec_1_get_field_end_info(rec, n);
		if(next_os & REC_1BYTE_SQL_NULL_MASK){ /*第1位表示是否是空*/
			*len = UNIV_SQL_NULL;
			return rec + os;
		}

		next_os = next_os & ~REC_1BYTE_SQL_NULL_MASK;
	}
	else{
		os = rec_2_get_field_start_offs(rec, n);
		next_os = rec_2_get_field_end_info(rec, n);
		if(next_os & REC_2BYTE_SQL_NULL_MASK){
			*len = UNIV_SQL_NULL;
			return rec + os;
		}

		next_os = next_os & ~(REC_1BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK);
	}

	*len = next_os - os;
	return rec + os;
}

/*设置列是否为空*/
void rec_set_nth_field_null_bit(rec_t* rec, ulint i, ibool val)
{
	ulint info;
	
	if(rec_get_1byte_offs_flag(rec)){
		info = rec_1_get_field_end_info(rec, i);
		if(val)
			info = info | REC_1BYTE_SQL_NULL_MASK;
		else
			info = info & ~REC_1BYTE_SQL_NULL_MASK;
		rec_1_set_field_end_info(rec, i, info);
	}
	else{
		info = rec_2_get_field_end_info(rec, i);
		if(val)
			info = info | REC_2BYTE_SQL_NULL_MASK;
		else
			info = info & ~REC_2BYTE_SQL_NULL_MASK;
		rec_2_set_field_end_info(rec, i, info);
	}
}

/*设置扩展位*/
void rec_set_nth_field_extern_bit(rec_t* rec, ulint i, ibool val, mtr_t* mtr)
{
	ulint info;

	ut_a(!rec_get_1byte_offs_flag(rec));
	ut_a(i < rec_get_n_fields(rec));

	info = rec_2_get_field_end_info(rec, i);

	if(val)
		info = info | REC_2BYTE_EXTERN_MASK;
	else
		info = info & ~REC_2BYTE_EXTERN_MASK;

	/*写入一条redo log*/
	if(mtr)
		mlog_write_ulint(rec - REC_N_EXTRA_BYTES - 2 * (i + 1), info, MLOG_2BYTES, mtr);
	else
		rec_2_set_field_end_info(rec, i, info);
}

void rec_set_field_extern_bits(rec_t* rec, ulint* vec, ulint n_fields, mtr_t* mtr)
{
	ulint i;
	for(i = 0; i < n_fields; i ++)
		rec_set_nth_field_extern_bit(rec, i, vec[i], mtr);
}

/*将第N个列用0填充*/
void rec_set_nth_field_sql_null(rec_t* rec, ulint n)
{
	ulint offset;

	offset = rec_get_field_start_offs(rec, n);
	data_write_sql_null(rec + offset, rec_get_nth_field_size(rec, n));
	rec_set_nth_field_null_bit(rec, n, TRUE);
}

/*将tuple内存中的逻辑记录转换成物理记录rec_t(在destination空间上)*/
rec_t* rec_convert_dtuple_to_rec_low(byte* destination, dtuple_t* tuple, ulint data_size)
{
	dfield_t* 	field;
	ulint		n_fields;
	rec_t* 		rec;
	ulint		end_offset;
	ulint		ored_offset;
	byte*		data;
	ulint		len;
	ulint		i;

	ut_ad(destination && dtuple);
	ut_ad(dtuple_validate(dtuple));
	ut_ad(dtuple_check_typed(dtuple));
	ut_ad(dtuple_get_data_size(dtuple) == data_size);
	
	n_fields = dtuple_get_n_fields(tuple);
	ut_ad(n_fields > 0);

	/*得到rec_t指针*/
	rec = destination + rec_get_converted_extra_size(data_size, n_fields);

	rec_set_n_fields(n_fields);
	rec_set_info_bits(rec, dtuple_get_info_bits(tuple));

	/*数据赋值*/
	if(data_size <= REC_1BYTE_OFFS_LIMIT){
		/*设置列的占位标识*/
		rec_set_1byte_offs_flag(dtuple, TRUE);
		/*数据拷贝*/
		for(i = 0; i < n_fields; i ++){
			field = dtuple_get_nth_field(dtuple, i);
			data = dfield_get_data(field);
			len = dfield_get_len(field);
			if(len == UNIV_SQL_NULL){
				len = dtype_get_sql_null_size(dfield_get_type(field));
				data_write_sql_null(rec + end_offset, len);

				end_offset += len;
				ored_offset = end_offset | REC_1BYTE_SQL_NULL_MASK;
			}
			else{
				ut_memcpy(rec + end_offset, data, len);
				end_offset += len;
				ored_offset = end_offset;
			}
			/*设置列的间隔偏移*/
			rec_1_set_field_end_info(rec, i, ored_offset);
		}
	}
	else{ /*列长度以2字节为记*/
		rec_set_1byte_offs_flag(dtuple, FALSE);
		for(i = 0; i < n_fields; i ++){
			field = dtuple_get_nth_field(tuple, i);
			data = dfield_get_data(field);
			len = dfield_get_len(field);
			if(len == UNIV_SQL_NULL){
				len = dtype_get_sql_null_size(dfield_get_type(field));
				data_write_sql_null(rec + end_offset, len);

				end_offset += len;
				ored_offset = end_offset | REC_2BYTE_SQL_NULL_MASK;
			}
			else{
				ut_memcpy(rec + end_offset, data, len);
				end_offset += len;
				ored_offset = end_offset;
			}

			rec_2_set_field_end_info(rec, i, ored_offset);
		}
	}

	ut_ad(rec_validate(rec));

	return rec;
}

/*将物理记录rec中的n_fields个列数据拷贝到tuple当中*/
void rec_copy_prefix_to_dtuple(dtuple_t* tuple, rec_t* rec, ulint n_fields, mem_heap_t* heap)
{
	dfield_t*	field;
	byte*		data;
	ulint		len;
	byte*		buf = NULL;
	ulint		i;

	ut_ad(rec_validate(rec));
	ut_ad(dtuple_check_typed(tuple));

	dtuple_set_info_bits(tuple, rec_get_info_bits(rec));
	for(i = 0; i < n_fields; i ++){
		field = dtuple_get_nth_field(tuple, i);
		data = rec_get_nth_field(rec, i, &len);
		if(len != UNIV_SQL_NULL){
			buf = mem_heap_alloc(heap, len);
			ut_memcpy(buf, data, len);
		}

		dfield_set_data(tuple, buf, len);
	}
}

/*将物理记录rec的内容拷贝到一个缓冲区中，并构造一个copy rec物理记录返回*/
rec_t* rec_copy_prefix_to_buf(rec_t* rec, ulint n_fields, byte** buf, ulint* buf_size)
{
	rec_t*	copy_rec;
	ulint	area_start;
	ulint	area_end;
	ulint	prefix_len;

	ut_ad(rec_validate(rec));

	area_end = rec_get_field_start_offs(rec, n_fields);
	if(rec_get_1byte_offs_flag(rec))
		area_start = REC_N_EXTRA_BYTES + n_fields;
	else
		area_start = REC_N_EXTRA_BYTES + 2 * n_fields;

	prefix_len = area_start + area_end;
	if((*buf == NULL) || (*buf_size < prefix_len)){
		if(*buf != NULL)
			mem_free(*buf);

		*buf = mem_alloc(prefix_len);
		*buf_size = prefix_len;
	}

	ut_memcpy(*buf, rec - area_start, prefix_len);
	copy_rec = *buf + area_start;

	rec_set_n_fields(copy_rec, n_fields);
}

ibool rec_validate(rec_t* rec)
{
	byte*	data;
	ulint	len;
	ulint	n_fields;
	ulint	len_sum	= 0;
	ulint	sum	= 0;
	ulint	i;

	ut_a(rec);
	n_fields = rec_get_n_fields(rec);

	if ((n_fields == 0) || (n_fields > REC_MAX_N_FIELDS)){
		fprintf(stderr, "InnoDB: Error: record has %lu fields\n", n_fields);
		return(FALSE);
	}

	for(i = 0; i < n_fields; i ++){
		/*校验列长度*/
		data = rec_get_nth_field(rec, i, &len);
		if(!(len < UNIV_PAGE_SIZE || len == UNIV_SQL_NULL)){
			fprintf(stderr, "InnoDB: Error: record field %lu len %lu\n", i, len);
			return FALSE;
		}

		if(len != UNIV_SQL_NULL){
			len_sum += len;
			sum += *(data + len - 1);
		}
		else
			len_sum += rec_get_nth_field_size(rec, i);
	}

	if(len_sum != (ulint)(rec_get_end(rec) - rec)){
		fprintf(stderr, "InnoDB: Error: record len should be %lu, len %lu\n", len_sum, (ulint)(rec_get_end(rec) - rec));
		return(FALSE);
	}

	rec_dummy = sum;

	return TRUE;
}

/*打印一个rec_t结构*/
void rec_print(rec_t* rec)
{
	byte*	data;
	ulint	len;
	char*	offs;
	ulint	n;
	ulint	i;

	ut_ad(rec);

	if (rec_get_1byte_offs_flag(rec))
		offs = "TRUE";
	else
		offs = "FALSE";

	n = rec_get_n_fields(rec);

	printf("PHYSICAL RECORD: n_fields %lu; 1-byte offs %s; info bits %lu\n", n, offs, rec_get_info_bits(rec));

	for (i = 0; i < n; i++) {
		data = rec_get_nth_field(rec, i, &len);

		printf(" %lu:", i);	

		if (len != UNIV_SQL_NULL) {
			if (len <= 30)
				ut_print_buf(data, len);
			else{
				ut_print_buf(data, 30);
				printf("...(truncated)");
			}
		} 
		else
			printf(" SQL NULL, size %lu ", rec_get_nth_field_size(rec, i));

		printf(";");
	}

	printf("\n");
	rec_validate(rec);
}

ulint rec_sprintf(char*	buf, ulint buf_len, rec_t* rec)	
{
	byte*	data;
	ulint	len;
	ulint	k, n, i;

	ut_ad(rec);
	n = rec_get_n_fields(rec);
	k = 0;

	if (k + 30 > buf_len)
		return(k);

	k += sprintf(buf + k, "RECORD: info bits %lu", rec_get_info_bits(rec));

	for (i = 0; i < n; i++) {
		if (k + 30 > buf_len) 
			return(k);

		data = rec_get_nth_field(rec, i, &len);
		k += sprintf(buf + k, " %lu:", i);
		if (len != UNIV_SQL_NULL) {
			if (k + 30 + 5 * len > buf_len)
				return(k);

			k += ut_sprintf_buf(buf + k, data, len);
		} 
		else
			k += sprintf(buf + k, " SQL NULL");

		k += sprintf(buf + k, ";");
	}

	return(k);
}


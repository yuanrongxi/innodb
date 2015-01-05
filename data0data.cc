#include "data0data.h"

#include "ut0rnd.h"
#include "rem0rec.h"
#include "page0page.h"
#include "dict0dict.h"
#include "btr0cur.h"


byte		data_error;
ulint		data_dummy;

byte		data_buf[8192];
ulint		data_rnd = 756511;

void dfield_set_data_noninline(dfield_t* field, void* data, ulint len)
{
	dfield_set_data(field, data, len);
}

void* dfield_get_data(dfield_t* field)
{
	return dfield_get_data(field);
}

ulint dfield_get_len_noninline(dfield_t* field)
{
	return dfield_get_len(field);
}

dfield_t* dtuple_get_nth_field_noninline(dtuple_t* tuple, ulint n)
{
	return dtuple_get_nth_field(tuple, n);
}

ulint dtuple_get_n_fields_noninline(dtuple_t* tuple)
{
	return dtuple_get_n_fields(tuple);
}

/*判断field中的数据是否与data相同*/
ibool dfield_data_is_binary_equal(dfield_t* field, ulint len, byte* data)
{
	if(field->len != len)
		return FALSE;

	if(len == UNIV_SQL_NULL)
		return FALSE;

	if(ut_memcmp(data, field->data, len) != 0)
		return FALSE;

	return TRUE;
}

/*比较tuple1是否和tuple2的field数据相同*/
ibool dtuple_datas_are_ordering_equal(dtuple_t* tuple1, dtuple_t* tuple2)
{
	dfield_t*	field1;
	dfield_t*	field2;
	ulint		n_fields;
	ulint		i;

	ut_ad(tuple1 && tuple2);
	ut_ad(tuple1->magic_n = DATA_TUPLE_MAGIC_N);
	ut_ad(tuple2->magic_n = DATA_TUPLE_MAGIC_N);
	ut_ad(dtuple_check_typed(tuple1));
	ut_ad(dtuple_check_typed(tuple2));

	/*判断field数量是否相同*/
	n_fields = dtuple_get_n_fields(tuple1);
	if(n_fields != dtuple_get_n_fields(tuple2))
		return FALSE;

	for(i = 0; i < n_fields; i ++){
		field1 = dtuple_get_nth_field(tuple1, i);
		field2 = dtuple_get_nth_field(tuple2, i);
		if(cmp_dfield_dfield(field1, field2) != 0)
			return FALSE;
	}

	return TRUE;
}

/*建立一个MYSQL的内存记录tuple,并分配一个mem_heap*/
dtuple_t* dtuple_create_for_mysql(void** heap, ulint n_fields)
{
	*heap = (void *)mem_heap_create(500);
	return dtuple_create(*((mem_heap_t **)heap), n_fields);
}

void dtuple_free_for_mysql(void* heap)
{
	mem_heap_free((mem_heap_t *)heap);
}

/*设置tuple的field数量*/
void dtuple_set_n_fields(dtuple_t* tuple, ulint n_fields)
{
	ut_ad(tuple);

	tuple->n_fields = n_fields;
	tuple->n_fields_cmp = n_fields;
}

/*检查field数据类型的合法性*/
static ibool dfield_check_typed_no_assert(dfield_t* field)
{
	if(dfield_get_type(field)->mtype > DATA_MYSQL || dfield_get_type(field)->mtype < DATA_VARCHAR){
		fprintf(stderr, "InnoDB: Error: data field type %lu, len %lu\n", dfield_get_type(field)->mtype, dfield_get_len(field));
		return FALSE;
	}

	return TRUE;
}

ibool dtuple_check_typed_no_assert(dtuple_t* tuple)
{
	dfield_t*	field;
	ulint	 	i;
	char		err_buf[1000];

	/*对field数量进行判断*/
	if(dtuple_get_n_fields(tuple) > REC_MAX_N_FIELDS){
		fprintf(stderr, "InnoDB: Error: index entry has %lu fields\n", dtuple_get_n_fields(tuple));
		dtuple_sprintf(err_buf, 900, tuple);
		fprintf(stderr,"InnoDB: Tuple contents: %s\n", err_buf);

		return FALSE;
	}
	
	for(i = 0; i < dtuple_get_n_fields(tuple); i ++){
		field = dtuple_get_nth_field(tuple, i);
		/*对各列进行合法性判断*/
		if(!dfield_check_typed_no_assert(field)){
			dtuple_sprintf(err_buf, 900, tuple);
			fprintf(stderr,"InnoDB: Tuple contents: %s\n", err_buf);	

			return(FALSE);
		}
	}

	return TRUE;
}

/*检查field数据类型合法性*/
ibool dfield_check_typed(dfield_t* field)
{
	if (dfield_get_type(field)->mtype > DATA_MYSQL || dfield_get_type(field)->mtype < DATA_VARCHAR) {
		fprintf(stderr,"InnoDB: Error: data field type %lu, len %lu\n",dfield_get_type(field)->mtype, dfield_get_len(field));

		ut_a(0);
	}

	return TRUE;
}

/*检查tuple的合法性*/
ibool dtuple_check_typed(dtuple_t* tuple)
{
	dfield_t* field;
	ulint i;
	
	for(i = 0; i < dtuple_get_n_fields(tuple); i ++){
		field = dtuple_get_nth_field(tuple, i);
		ut_a(dfield_check_typed(field));
	}

	return TRUE;
}

ibool dtuple_validate(dtuple_t* tuple)
{
	dfield_t*	field;
	byte*	 	data;
	ulint	 	n_fields;
	ulint	 	len;
	ulint	 	i;
	ulint	 	j;

	ut_a(tuple->magic_n == DATA_TUPLE_MAGIC_N);
	n_fields = dtuple_get_n_fields(tuple);

	for(i = 0; i < n_fields; i ++){
		field = dtuple_get_nth_field(tuple, i);
		len = dfield_get_len(field);

		if(len != UNIV_SQL_NULL){
			data = (byte *)(field->data);
			for(j = 0; j < len; j ++){
				data_dummy += *data;
				data ++;
			}
		}
	}

	ut_a(dtuple_check_typed);
	return TRUE;
}

/*对field进行打印*/
void dfield_print(dfield_t*	dfield)
{
	byte*	data;
	ulint	len;
	ulint	mtype;
	ulint	i;

	len = dfield_get_len(dfield);
	data = dfield_get_data(dfield);

	if (len == UNIV_SQL_NULL) {
		printf("NULL");
		return;
	}

	mtype = dtype_get_mtype(dfield_get_type(dfield));

	if ((mtype == DATA_CHAR) || (mtype == DATA_VARCHAR)) {
		for (i = 0; i < len; i++) {
			if (isprint((char)(*data)))
				printf("%c", (char)*data);
			else
				printf(" ");

			data++;
		}
	} 
	else if (mtype == DATA_INT) {
		ut_a(len == 4);
		printf("%i", (int)mach_read_from_4(data));
	} 
	else {
		ut_error;
	}
}

void dfield_print_also_hex(dfiled_t* field)
{
	byte*	data;
	ulint	len;
	ulint	mtype;
	ulint	i;
	ibool	print_also_hex;

	len = dfield_get_len(field);
	data = dfield_get_data(field);

	if(len == UNIV_SQL_NULL){
		printf("NULL");
		return;
	}

	mtype = dtype_get_mtype(dfield_get_type(dfield));
	if((mtype == DATA_CHAR) || (mtype == DATA_VARCHAR)){
		print_also_hex = FALSE;
		
		for(i = 0; i < len; i ++){
			if (isprint((char)(*data))) {
				printf("%c", (char)*data);
			} 
			else {
				print_also_hex = TRUE;
				printf(" ");
			}

			data ++;
		}

		if(!print_also_hex){
			return ;
		}

		printf(" Hex: ");
		data = dfield_get_data(field);
		for(i = 0; i < len; i ++){
			printf("%02lx", (ulint)*data);
			data++;
		}
	}
	else if(mtype == DATA_INT){
		ut_a(len == 4);
		printf("%i", (int)mach_read_from_4(data));
	}
	else{
		ut_error;
	}
}

void dtuple_print(dtuple_t*	tuple)
{
	dfield_t*	field;
	ulint		n_fields;
	ulint		i;

	n_fields = dtuple_get_n_fields(tuple);

	printf("DATA TUPLE: %lu fields;\n", n_fields);

	for (i = 0; i < n_fields; i++) {
		printf(" %lu:", i);	

		field = dtuple_get_nth_field(tuple, i);

		if (field->len != UNIV_SQL_NULL) {
			ut_print_buf(field->data, field->len);
		} else {
			printf(" SQL NULL");
		}

		printf(";");
	}

	printf("\n");

	dtuple_validate(tuple);
}

/*将tuple的内容输入到buf中*/
ulint dtuple_sprintf(
	char*		buf,	/* in: print buffer */
	ulint		buf_len,/* in: buf length in bytes */
	dtuple_t*	tuple)	/* in: tuple */
{
	dfield_t*	field;
	ulint		n_fields;
	ulint		len;
	ulint		i;

	len = 0;

	n_fields = dtuple_get_n_fields(tuple);

	for (i = 0; i < n_fields; i++) {
		if (len + 30 > buf_len)
			return(len);

		len += sprintf(buf + len, " %lu:", i);	

		field = dtuple_get_nth_field(tuple, i);
		if (field->len != UNIV_SQL_NULL) {
			if (5 * field->len + len + 30 > buf_len)
				return(len);

			len += ut_sprintf_buf(buf + len, field->data, field->len);
		} 
		else
			len += sprintf(buf + len, " SQL NULL");

		len += sprintf(buf + len, ";");
	}

	return(len);
}

/*tuple转成big_rec_t*/
big_rec_t* dtuple_convert_big_rec(dict_index_t* index, dtuple_t* entry, ulint* ext_vec, ulint n_ext_vec)
{
	mem_heap_t*	heap;
	big_rec_t*	vector;
	dfield_t*	dfield;
	ulint		size;
	ulint		n_fields;
	ulint		longest;
	ulint		longest_i = ULINT_MAX;
	ibool		is_externally_stored;
	ulint		i;
	ulint		j;
	char		err_buf[1000];

	ut_a(dtuple_check_typed_no_assert(entry));
	size = rec_get_converted_size(entry);

	if(size > 1000000000){ /*1G*/
		fprintf(stderr,"InnoDB: Warning: tuple size very big: %lu\n", size);
		dtuple_sprintf(err_buf, 900, entry);
		fprintf(stderr, "InnoDB: Tuple contents: %s\n", err_buf);
	}

	/*建立一个分配堆*/
	heap = mem_heap_create(size + dtuple_get_n_fields(entry) * sizeof(big_rec_field_t) + 1000);
	vector = mem_heap_alloc(heap, dtuple_get_n_fields(entry) * sizeof(big_rec_field_t));
	vector->heap = heap;
	vector->fields = mem_heap_alloc(heap, dtuple_get_n_fields(entry) * sizeof(big_rec_field_t));

	n_fields = 0;

	/*检查tuple的记录大小，如果超过1/2个页空闲空间（除去页头页尾和infimum、supremum），就进行转换*/
	while((rec_get_converted_size(entry) >= page_get_free_space_of_empty() / 2) || rec_get_converted_size(entry) > REC_MAX_DATA_SIZE){
		longest = 0;
		for(i = dict_index_get_n_unique_in_tree(index); i < dtuple_get_n_fields(entry); i ++){
			/*跳过外部已经存储了的field*/
			is_externally_stored = FALSE;
			if(ext_vec){
				for(j = 0; j < n_ext_vec; j ++)
					if(ext_vec[j] = i)
						is_externally_stored = TRUE;
			}

			if(!is_externally_stored && dict_field_get_col(dict_index_get_nth_field(index, i))->ord_part == 0){
				dfield = dtuple_get_nth_field(entry, i);
				if(dfield->len != UNIV_SQL_NULL && dfield->len > longest){
					longest = dfield->len;
					longest_i = i;
				}
			}
		}

		if(longest < BTR_EXTERN_FIELD_REF_SIZE + 10 + REC_1BYTE_OFFS_LIMIT){
			mem_heap_free(heap);
			return NULL;
		}

		/*一个field分成多个big_field_t*/
		dfield = dtuple_get_nth_field(entry, longest_i);
		vector->fields[n_fields].field_no = longest_i;
		/*确定big_field的数据长度*/
		vector->fields[n_fields].len = dfield->len - REC_1BYTE_OFFS_LIMIT;
		vector->fields[n_fields].data = mem_heap_alloc(heap, vector->fields[n_fields].len);

		ut_memcpy(vector->fields[n_fields].data, ((byte*)dfield->data) + dfield->len, vector->fields[n_fields].len);
		dfield->len = dfield->len - vector->fields[n_fields].len + BTR_EXTERN_FIELD_REF_SIZE;

		memset(((byte*)dfield->data)+ dfield->len - BTR_EXTERN_FIELD_REF_SIZE, 0, BTR_EXTERN_FIELD_REF_SIZE);

		n_fields ++;
	}

	vector->n_fields = n_fields;
	return vector;
}

/*将big_fields转换成dfield*/
void dtuple_convert_back_big_rec(dict_index_t* index, dtuple_t* entry, big_rec_t* vector)
{
	dfield_t*	dfield;
	ulint		i;

	for (i = 0; i < vector->n_fields; i++) {
		dfield = dtuple_get_nth_field(entry, vector->fields[i].field_no);

		/* Copy data from big rec vector */
		ut_memcpy(((byte*)dfield->data) + dfield->len - BTR_EXTERN_FIELD_REF_SIZE, vector->fields[i].data, vector->fields[i].len);
		dfield->len = dfield->len + vector->fields[i].len - BTR_EXTERN_FIELD_REF_SIZE;
	}	

	mem_heap_free(vector->heap);
}

void dtuple_big_rec_free(big_rec_t* vector)
{
	mem_heap_free(vector->heap);
}

#ifdef notdefined
static ulint dtuple_gen_rnd_ulint(ulint	n1,	ulint n2, ulint	n3)
{
	ulint 	m;
	ulint	n;

	m = ut_rnd_gen_ulint() % 16;
	if (m < 10)
		n = n1;
	else if (m < 15)
		n = n2; 
	else 
		n = n3;

	m = ut_rnd_gen_ulint();

	return(m % n);
}

dtuple_t* dtuple_gen_rnd_tuple(mem_heap_t* heap)
{
	ulint		n_fields;
	dfield_t*	field;
	ulint		len;
	dtuple_t*	tuple;	
	ulint		i;
	ulint		j;
	byte*		ptr;

	/*随机产生列数*/
	n_fields = dtuple_gen_rnd_ulint(5, 30, 300) + 1;

	/*创建一个tuple*/
	tuple = dtuple_create(heap, n_fields);
	for (i = 0; i < n_fields; i++) {
		/*进行field数据长度的随机产生*/
		if (n_fields < 7)
			len = dtuple_gen_rnd_ulint(5, 30, 400); 
		else
			len = dtuple_gen_rnd_ulint(7, 5, 17);

		field = dtuple_get_nth_field(tuple, i);
		if (len == 0)
			dfield_set_data(field, NULL, UNIV_SQL_NULL);
		else{
			ptr = mem_heap_alloc(heap, len);
			dfield_set_data(field, ptr, len - 1);
			/*设置field数据*/
			for (j = 0; j < len; j++) {
				*ptr = (byte)(65 + dtuple_gen_rnd_ulint(22, 22, 22));
				ptr++;
			}
		}

		dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 500, 0);
	}

	ut_a(dtuple_validate(tuple));
	return(tuple);
}

void dtuple_gen_test_tuple( dtuple_t* tuple, ulint i)	
{
	ulint		j;
	dfield_t*	field;
	void*		data = NULL;
	ulint		len	= 0;

	for (j = 0; j < 3; j++) {
		switch (i % 8) {
		case 0:
			data = ""; len = 0; break;
		case 1:
			data = "A"; len = 1; break;
		case 2:
			data = "AA"; len = 2; break;
		case 3:
			data = "AB"; len = 2; break;
		case 4:
			data = "B"; len = 1; break;
		case 5:
			data = "BA"; len = 2; break;
		case 6:
			data = "BB"; len = 2; break;
		case 7:
			len = UNIV_SQL_NULL; break;
		}

		field = dtuple_get_nth_field(tuple, 2 - j);

		dfield_set_data(field, data, len);
		dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

		i = i / 8;
	}

	ut_ad(dtuple_validate(tuple));
}

void dtuple_gen_test_tuple3(
	dtuple_t*	tuple,	/* in/out: a tuple with >= 3 fields */
	ulint		i,	/* in: a number < 1000000 */
	ulint		type,	/* in: DTUPLE_TEST_FIXED30, ... */
	byte*		buf)	/* in: a buffer of size >= 16 bytes */
{
	dfield_t*	field;
	ulint		third_size;

	ut_ad(tuple && buf);
	ut_ad(i < 1000000);

	field = dtuple_get_nth_field(tuple, 0);

	ut_strcpy((char*)buf, "0000000");

	buf[1] = (byte)('0' + (i / 100000) % 10);
	buf[2] = (byte)('0' + (i / 10000) % 10);
	buf[3] = (byte)('0' + (i / 1000) % 10);
	buf[4] = (byte)('0' + (i / 100) % 10);
	buf[5] = (byte)('0' + (i / 10) % 10);
	buf[6] = (byte)('0' + (i % 10));

	dfield_set_data(field, buf, 8);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	field = dtuple_get_nth_field(tuple, 1);

	i = i % 1000; /* ut_rnd_gen_ulint() % 1000000; */

	ut_strcpy((char*)buf + 8, "0000000");

	buf[9] = (byte)('0' + (i / 100000) % 10);
	buf[10] = (byte)('0' + (i / 10000) % 10);
	buf[11] = (byte)('0' + (i / 1000) % 10);
	buf[12] = (byte)('0' + (i / 100) % 10);
	buf[13] = (byte)('0' + (i / 10) % 10);
	buf[14] = (byte)('0' + (i % 10));

	dfield_set_data(field, buf + 8, 8);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	field = dtuple_get_nth_field(tuple, 2);

	data_rnd += 8757651;

	if (type == DTUPLE_TEST_FIXED30) {
		third_size = 30;
	} else if (type == DTUPLE_TEST_RND30) {
		third_size = data_rnd % 30;
	} else if (type == DTUPLE_TEST_RND3500) {
		third_size = data_rnd % 3500;
	} else if (type == DTUPLE_TEST_FIXED2000) {
		third_size = 2000;
	} else if (type == DTUPLE_TEST_FIXED3) {
		third_size = 3;
	} else {
		ut_error;
	}

	if (type == DTUPLE_TEST_FIXED30) {
		dfield_set_data(field,"12345678901234567890123456789", third_size);
	} else {
		dfield_set_data(field, data_buf, third_size);
	}

	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	ut_ad(dtuple_validate(tuple));
}

void dtuple_gen_search_tuple3(
	dtuple_t*	tuple,	/* in/out: a tuple with 1 or 2 fields */
	ulint		i,		/* in: a number < 1000000 */
	byte*		buf)	/* in: a buffer of size >= 16 bytes */
{
	dfield_t*	field;

	ut_ad(tuple && buf);
	ut_ad(i < 1000000);

	field = dtuple_get_nth_field(tuple, 0);

	ut_strcpy((char*)buf, "0000000");

	buf[1] = (byte)('0' + (i / 100000) % 10);
	buf[2] = (byte)('0' + (i / 10000) % 10);
	buf[3] = (byte)('0' + (i / 1000) % 10);
	buf[4] = (byte)('0' + (i / 100) % 10);
	buf[5] = (byte)('0' + (i / 10) % 10);
	buf[6] = (byte)('0' + (i % 10));

	dfield_set_data(field, buf, 8);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	if (dtuple_get_n_fields(tuple) == 1)
		return;

	field = dtuple_get_nth_field(tuple, 1);

	i = (i * 1000) % 1000000;

	ut_strcpy((char*)buf + 8, "0000000");

	buf[9] = (byte)('0' + (i / 100000) % 10);
	buf[10] = (byte)('0' + (i / 10000) % 10);
	buf[11] = (byte)('0' + (i / 1000) % 10);
	buf[12] = (byte)('0' + (i / 100) % 10);
	buf[13] = (byte)('0' + (i / 10) % 10);
	buf[14] = (byte)('0' + (i % 10));

	dfield_set_data(field, buf + 8, 8);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	ut_ad(dtuple_validate(tuple));
}

void dtuple_gen_test_tuple_TPC_A(
	dtuple_t*	tuple,	/* in/out: a tuple with >= 3 fields */
	ulint		i,		/* in: a number < 10000 */
	byte*		buf)	/* in: a buffer of size >= 16 bytes */
{
	dfield_t*	field;
	ulint		third_size;

	ut_ad(tuple && buf);
	ut_ad(i < 10000);

	field = dtuple_get_nth_field(tuple, 0);

	ut_strcpy((char*)buf, "0000");

	buf[0] = (byte)('0' + (i / 1000) % 10);
	buf[1] = (byte)('0' + (i / 100) % 10);
	buf[2] = (byte)('0' + (i / 10) % 10);
	buf[3] = (byte)('0' + (i % 10));

	dfield_set_data(field, buf, 5);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	field = dtuple_get_nth_field(tuple, 1);

	dfield_set_data(field, buf + 8, 5);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	field = dtuple_get_nth_field(tuple, 2);

	third_size = 90;

	dfield_set_data(field, data_buf, third_size);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	ut_ad(dtuple_validate(tuple));
}

void dtuple_gen_search_tuple_TPC_A(
	dtuple_t*	tuple,	/* in/out: a tuple with 1 field */
	ulint		i,		/* in: a number < 10000 */
	byte*		buf)	/* in: a buffer of size >= 16 bytes */
{
	dfield_t*	field;

	ut_ad(tuple && buf);
	ut_ad(i < 10000);

	field = dtuple_get_nth_field(tuple, 0);

	ut_strcpy((char*)buf, "0000");

	buf[0] = (byte)('0' + (i / 1000) % 10);
	buf[1] = (byte)('0' + (i / 100) % 10);
	buf[2] = (byte)('0' + (i / 10) % 10);
	buf[3] = (byte)('0' + (i % 10));

	dfield_set_data(field, buf, 5);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	ut_ad(dtuple_validate(tuple));
}

void dtuple_gen_test_tuple_TPC_C(
	dtuple_t*	tuple,	/* in/out: a tuple with >= 12 fields */
	ulint		i,		/* in: a number < 100000 */
	byte*		buf)	/* in: a buffer of size >= 16 bytes */
{
	dfield_t*	field;
	ulint		size;
	ulint		j;

	ut_ad(tuple && buf);
	ut_ad(i < 100000);

	field = dtuple_get_nth_field(tuple, 0);

	buf[0] = (byte)('0' + (i / 10000) % 10);
	buf[1] = (byte)('0' + (i / 1000) % 10);
	buf[2] = (byte)('0' + (i / 100) % 10);
	buf[3] = (byte)('0' + (i / 10) % 10);
	buf[4] = (byte)('0' + (i % 10));

	dfield_set_data(field, buf, 5);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	field = dtuple_get_nth_field(tuple, 1);

	dfield_set_data(field, buf, 5);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	for (j = 0; j < 10; j++) {
		field = dtuple_get_nth_field(tuple, 2 + j);

		size = 24;

		dfield_set_data(field, data_buf, size);
		dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);
	}

	ut_ad(dtuple_validate(tuple));
}

void dtuple_gen_search_tuple_TPC_C(
	dtuple_t*	tuple,	/* in/out: a tuple with 1 field */
	ulint		i,		/* in: a number < 100000 */
	byte*		buf)	/* in: a buffer of size >= 16 bytes */
{
	dfield_t*	field;

	ut_ad(tuple && buf);
	ut_ad(i < 100000);

	field = dtuple_get_nth_field(tuple, 0);

	buf[0] = (byte)('0' + (i / 10000) % 10);
	buf[1] = (byte)('0' + (i / 1000) % 10);
	buf[2] = (byte)('0' + (i / 100) % 10);
	buf[3] = (byte)('0' + (i / 10) % 10);
	buf[4] = (byte)('0' + (i % 10));

	dfield_set_data(field, buf, 5);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 100, 0);

	ut_ad(dtuple_validate(tuple));
}

#endif




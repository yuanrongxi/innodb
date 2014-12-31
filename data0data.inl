#include "mem0mem.h"
#include "ut0rnd.h"


UNIV_INLINE dtype_t* dfield_get_type(dfield_t* field)
{
	ut_ad(field);
	return &(field->type);
}

UNIV_INLINE dtype_t* dfield_set_type(dfield_t* field, dtype_t* type)
{
	ut_ad(field);
	field->type = type;
}

UNIV_INLINE void* dfield_get_data(dfield_t* field)
{
	ut_ad(field);
	ut_ad(field->len == UNIV_SQL_NULL || field->data != &data_error);

	return field->data;
}

UNIV_INLINE ulint dfield_get_len(dfield_t* field)
{
	ut_ad(field);
	ut_ad((field->len == UNIV_SQL_NULL) || (field->data != &data_error));

	return field->len;
}

UNIV_INLINE void dfield_set_len(dfield_t* field, ulint len)
{
	ut_ad(field);
	field->len = len;
}

UNIV_INLINE void dfield_set_data(dfield_t* field, ulint len)
{
	ut_ad(filed);
	field->len = len;
	field->data = data;
}

UNIV_INLINE void dfield_copy_data(dfield_t* field1, dfield_t* field2)
{
	ut_ad(field1 && field2);
	field1->len = field2->len;
	field1->data = field2->data;
}

UNIV_INLINE void dfield_copy(dfield_t* field1, dfield_t* field2)
{
	*field1 = *field2;
}

UNIV_INLINE ibool dfield_datas_are_binary_equal(dfield_t* field1, dfield_t* field2)
{
	if(field1->len != field2->len || (field1->len != UNIV_SQL_NULL && (ut_memcmp(field1->data, field2->data, field1->len) != 0)))
		return FALSE;
	
	return TRUE;
}

UNIV_INLINE ulint dtuple_get_info_bits(dtuple_t* tuple)
{
	ut_ad(tuple);
	return tuple->info_bits;
}

UNIV_INLINE void  dtuple_set_info_bits(dtuple_t* tuple, ulint info_bits)
{
	ut_ad(tuple);
	return tuple->info_bits = info_bits;
}

UNIV_INLINE ulint dtuple_get_n_fields_cmp(dtuple_t* tuple)
{
	ut_ad(tuple);
	return tuple->n_fields_cmp;
}

UNIV_INLINE void dtuple_set_n_fields_cmp(dtuple_t* tuple, ulint n_fields_cmp)
{
	ut_ad(tuple);
	ut_ad(n_fields_cmp <= tuple->n_fields);

	tuple->n_fields_cmp = n_fields_cmp;
}

UNIV_INLINE ulint dtuple_get_n_fields(dtuple_t* tuple)
{
	ut_ad(tuple);
	return tuple->n_fields;
}

UNIV_INLINE dfield_t* dtuple_get_nth_field(dtuple_t* tuple, ulint n)
{
	ut_ad(tuple);
	ut_ad(n < tuple->n_fields);

	return tuple->fields + n;
}

UNIV_INLINE dtuple_t* dtuple_create(mem_heap_t* heap, ulint n_fields)
{
	dtuple_t* tuple;
	ut_ad(heap);

	tuple = (dtuple_t*) mem_heap_alloc(heap, sizeof(dtuple_t)+ n_fields * sizeof(dfield_t));

	tuple->info_bits = 0;
	tuple->n_fields = n_fields;
	tuple->n_fields_cmp = n_fields;
	tuple->fields = (dfield_t*)(((byte*)tuple) + sizeof(dtuple_t));

#ifdef UNIV_DEBUG
	tuple->magic_n = DATA_TUPLE_MAGIC_N;
	{
		ulint i;
		/*进行field初始化*/
		for(i = 0; i < n_fields; i ++){
			(tuple->fields + i)->data = &data_error;
			dfield_get_type(tuple->fields + i)->mtype = DATA_ERROR;
		}
	}
#endif

	return tuple;
}

/*获得tuple中的数据中的总长度*/
UNIV_INLINE ulint dtuple_get_data_size(dtuple_t* tuple)
{
	dfield_t*	field;
	ulint	 	n_fields;
	ulint	 	len;
	ulint	 	i;
	ulint	 	sum	= 0;

	ut_ad(tuple);
	ut_ad(dtuple_check_typed(tuple));
	ut_ad(tuple->magic_n = DATA_TUPLE_MAGIC_N);

	n_fields = tuple->n_fields;

	for(i = 0; i < n_fields; i ++){
		field = dtuple_get_nth_field(tuple, i);
		len = dfield_get_len(field);

		if (len == UNIV_SQL_NULL) {
			len = dtype_get_sql_null_size(dfield_get_type(field));
		}

		sum += len;
	}

	return sum;
}

/*将0 ~ n全部设置成DATA_BINARY类型*/
UNIV_INLINE void dtuple_set_types_binary(dtuple_t* tuple, ulint n)
{
	dtype_t*	dfield_type;
	ulint		i;

	for(i = 0; i < n; i ++){
		dfield_type = dfield_get_type(dtuple_get_nth_field(tuple, i));
		dtype_set(dfield_type, DATA_BINARY, 0, 0, 0);
	}
}

UNIV_INLINE ulint dtuple_fold(dtuple_t* tuple, ulint n_fields, ulint n_bytes, dulint tree_id)
{
	dfield_t*	field;
	ulint		i;
	byte*		data;
	ulint		len;
	ulint		fold;

	ut_ad(tuple);
	ut_ad(tuple->magic_n = DATA_TUPLE_MAGIC_N);
	ut_ad(dtuple_check_typed(tuple));

	fold = ut_fold_dulint(tree_id);

	/*计算HASH?*/
	for(i = 0; i < n_fields; i ++){
		field = dtuple_get_nth_field(tuple, i);
		data = (byte *)dfield_get_data(field);
		len = dfield_get_len(field);

		if(len != UNIV_SQL_NULL)
			fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
	}

	if(n_bytes > 0){ /*如果n_bytes > 0,后面一个field也加入*/
		field = dtuple_get_nth_field(tuple, i);

		data = (byte*) dfield_get_data(field);
		len = dfield_get_len(field);

		if (len != UNIV_SQL_NULL) {
			if (len > n_bytes)
				len = n_bytes;

			fold = ut_fold_ulint_pair(fold, ut_fold_binary(data, len));
		}
	}

	return fold;
}

UNIV_INLINE void data_write_sql_null(byte* data, ulint len)
{
	ulint j;

	for (j = 0; j < len; j++) 
		data[j] = '\0';
}


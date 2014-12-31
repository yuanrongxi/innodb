#ifndef __data0data_h_
#define __data0data_h_

#include "univ.h"

#include "data0types.h"
#include "data0type.h"
#include "mem0mem.h"
#include "dict0types.h"

/*tuple magic number*/
#define DATA_TUPLE_MAGIC_N	65478679

#define DTUPLE_TEST_FIXED30	1
#define DTUPLE_TEST_RND30	2
#define DTUPLE_TEST_RND3500	3
#define DTUPLE_TEST_FIXED2000	4
#define DTUPLE_TEST_FIXED3	5

struct dfield_struct
{
	void*			data;		/*field数据*/
	ulint			len;		/*数据长度*/
	dtype_t			type;		/*数据类型*/
	ulint			col_no;		/*列序号*/
};

struct dtuple_struct
{
	ulint			info_bits;
	ulint			n_fields;
	ulint			n_fields_cmp;
	dfield_t*		fields;

	UT_LIST_NODE_T(dtuple_t) tuple_list;
	ulint			magic_n;
};

/*超大的field*/
typedef struct big_rec_field_struct
{
	ulint		field_no;	/*field number*/
	ulint		len;		/*存储的数据长度*/
	byte*		data;		/*存储的数据*/
}big_rec_field_t;

typedef struct big_rec_struct 
{
	mem_heap_t*			heap;		/*内存分配堆*/
	ulint				n_fields;	/*field数*/
	big_rec_field_t*	fields;		/*field的数组*/
}big_rec_t;

void					dfield_set_data_noninline(dfield_t* field, void* data, ulint len);
void*					dfield_get_data_noninline(dfield_t* field);

ulint					dfield_get_len_noninline(dfield_t* field);			
ulint					dtuple_get_n_fields_noninline(dtuple_t* tuple);

dfield_t*				dtuple_get_nth_field_noninline(dtuple_t* tuple, ulint n);			

UNIV_INLINE	dtype_t*	dfield_get_type(dfield_t* field);
UNIV_INLINE void		dfield_set_type(dfield_t* field, dtype_t* type);

UNIV_INLINE void*		dfield_get_data(dfield_t* field);
UNIV_INLINE ulint		dfield_get_len(dfield_t* field);
UNIV_INLINE void		dfield_set_len(dfield_t* field, ulint len);
UNIV_INLINE void		dfield_set_data(dfield_t* field, ulint len);

UNIV_INLINE void		data_write_sql_null(byte* data, ulint len);

UNIV_INLINE void		dfield_copy_data(dfield_t* field1, dfield_t* field2);
UNIV_INLINE void		dfield_copy(dfield_t* field1, dfield_t* field2);

UNIV_INLINE ibool		dfield_datas_are_binary_equal(dfield_t* field1, dfield_t* field2);
ibool					dfield_data_is_binary_equal(dfield_t* field, ulint len, byte* data);

UNIV_INLINE ulint		dtuple_get_n_fields(dtuple_t* tuple);
UNIV_INLINE ulint		dtuple_get_n_fields(dtuple_t* tuple);
UNIV_INLINE dfield_t*	dtuple_get_nth_field(dtuple_t* tuple, ulint n);

UNIV_INLINE ulint		dtuple_get_info_bits(dtuple_t* tuple);
UNIV_INLINE void		dtuple_set_info_bits(dtuple_t* tuple, ulint info_bits);

UNIV_INLINE ulint		dtuple_get_n_fields_cmp(dtuple_t* tuple);
UNIV_INLINE void		dtuple_set_n_fields_cmp(dtuple_t* tuple, ulint n_fields_cmp);

UNIV_INLINE dtuple_t*	dtuple_create(mem_heap_t* heap, ulint n_fields);

dtuple_t*				dtuple_create_for_mysql(void** heap, ulint n_fields);
void					dtuple_free_for_mysql(void* heap);

void					dtuple_set_n_fields(dtuple_t* tuple, ulint n_fields);
UNIV_INLINE ulint		dtuple_get_data_size(dtuple_t* tuple);

ibool					dtuple_datas_are_ordering_equal(dtuple_t* tuple1, dtuple_t* tuple2);
UNIV_INLINE ulint		dtuple_fold(dtuple_t* tuple, ulint n_fields, ulint n_bytes, dulint tree_id);
UNIV_INLINE void		dtuple_set_types_binary(dtuple_t* tuple, ulint n);

ibool					dfield_check_typed(dfield_t* field);
ibool					dtuple_check_typed(dtuple_t* tuple);
ibool					dtuple_check_typed_no_assert(dtuple_t* tuple);

ibool					dtuple_validate(dtuple_t* tuple);
void					dfield_print(dfield_t* field);

void					dfield_print_also_hex(dfiled_t* field);
void					dtuple_print(dtuple_t* tuple);
ulint					dtuple_sprintf(char* buf, ulint buf_len, dtuple_t* tuple);

big_rec_t*				dtuple_convert_big_rec(dict_index_t* index, dtuple_t* entry, ulint* ext_vec, ulint n_ext_vec);
void					dtuple_convert_back_big_rec(dict_index_t* index, dtuple_t* entry, big_rec_t* vector);
void					dtuple_big_rec_free(big_rec_t* vector);
dtuple_t*				dtuple_gen_rnd_tuple(mem_heap_t* heap);
void					dtuple_gen_test_tuple(dtuple_t* tuple, ulint i);
void					dtuple_gen_search_tuple3(dtuple_t* tuple, ulint i, byte* buf);

void					dtuple_gen_test_tuple_TPC_A(dtuple_t* tuple, ulint i, byte* buf);
void					dtuple_gen_search_tuple_TPC_A(dtuple_t* tuple, ulint i, byte* buf);
void					dtuple_gen_test_tuple_TPC_C(dtuple_t* tuple, ulint i, byte* buf);
void					dtuple_gen_search_tuple_TPC_C(dtuple_t* tuple, ulint i, byte* buf);

#endif





#include "dict0dict.h"
#include "rem0rec.h"
#include "trx0undo.h"

dulint row_get_rec_sys_field(ulint type, rec_t* rec, dict_index_t* index);

void row_set_rec_sys_field(ulint type, rec_t* rec, dict_index_t* index, dulint val);

/*从记录中读取聚集索引最后一次操作的事务ID*/
UNIV_INLINE dulint row_get_rec_trx_id(rec_t* rec, dict_index_t* index)
{
	ulint	offset;

	ut_ad(index->type & DICT_CLUSTERED);

	/*从索引中得到trx id列的偏移*/
	offset = index->trx_id_offset;
	if(offset)
		return trx_read_trx_id(rec + offset);
	else
		return row_get_rec_sys_field(DATA_TRX_ID, rec, index);
}

/*获得记录中聚集索引记录的roll ptr*/
UNIV_INLINE dulint row_get_rec_roll_ptr(rec_t* rec, dict_index_t* index)
{
	ulint	offset;

	ut_ad(index->type & DICT_CLUSTERED);

	offset = index->trx_id_offset;
	if(offset)
		return trx_read_roll_ptr(rec + offset + DATA_TRX_ID_LEN); /*ROLL PTR在trx id列的后面*/
	else
		return row_get_rec_sys_field(DATA_ROLL_PTR, rec, index);
}

/*设置聚集索引记录的trx id*/
UNIV_INLINE void row_set_rec_trx_id(rec_t* rec, dict_index_t* index, dulint trx_id)
{
	ulint	offset;

	ut_ad(index->type & DICT_CLUSTERED);

	offset = index->trx_id_offset;
	if(offset)
		trx_write_trx_id(rec + offset, trx_id);

}

/*参考聚集索引的记录(tuple)构建一个辅助索引记录(rec),map是需要拷贝的列序号的数组列表
tuple -> rec_t*/
UNIV_INLINE void row_build_row_ref_fast(dtuple_t* ref, ulint* map, rec_t* rec)
{
	dfield_t*	dfield;
	byte*		field;
	ulint		len;
	ulint		ref_len;
	ulint		field_no;
	ulint		i;

	ref_len = dtuple_get_n_fields(ref);

	for(i = 0; i < ref_len; i ++){
		dfield = dtuple_get_nth_field(ref, i);
		field_no = *(map + i); /*获得需要拷贝的列对象*/

		if(field_no != ULINT_UNDEFINED){
			field = rec_get_nth_field(rec, field_no, &len);
			dfield_set_data(dfield, field, len);
		}
	}
}



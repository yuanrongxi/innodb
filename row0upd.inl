#include "mtr0log.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "row0row.h"
#include "btr0sea.h"

/*创建一个修改列内容的列序列对象*/
UNIV_INLINE upd_t* upd_create(ulint n, mem_heap_t* heap)
{
	upd_t* update;
	ulint i;

	update = mem_heap_alloc(heap, sizeof(upd_t));
	update->info_bits = 0;
	update->n_fields = n;
	update->fields = mem_heap_alloc(heap, sizeof(upd_field_t) * n);

	for(i = 0; i < n; i ++)
		update->fields[i].extern_storage = FALSE;

	return update;
}

/*获得update中列的数量*/
UNIV_INLINE ulint upd_get_n_fields(upd_t* update)
{
	ut_ad(update);
	return update->n_fields;
}

/*获得update中第n个修改列的对象*/
UNIV_INLINE upd_field_t* upd_get_nth_field(upd_t* update, ulint n)
{
	ut_ad(update);
	ut_ad(n < update->n_fields);

	return (update->fields + n);
}

/*将field_no设置到upd_field中，并将index对应的数据类型赋值给upd_field*/
UNIV_INLINE void upd_field_set_field_no(upd_field_t* upd_field, ulint field_no, dict_index_t* index)
{
	upd_field->field_no = field_no;
	if(field_no >= dict_index_get_n_fields(index)){
		fprintf(stderr, "InnoDB: Error: trying to access field %lu in table %s\n"
			"InnoDB: index %s, but index has only %lu fields\n",
			field_no, index->table_name, index->name, dict_index_get_n_fields(index));
	}

	dtype_copy(dfield_get_type(&(upd_field->new_val)), dict_index_get_nth_type(index, field_no));
}

/*设置聚集索引记录rec的trx id和roll ptr系统列*/
UNIV_INLINE void row_upd_rec_sys_fields(rec_t* rec, dict_index_t* index, trx_t* trx, dulint roll_ptr)
{
	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(!buf_block_align(rec)->is_hashed || rw_lock_own(&btr_search_latch, RW_LOCK_EX));

	row_set_rec_trx_id(rec, index, trx->id);
	row_set_rec_roll_ptr(rec, index, roll_ptr);
}




#ifndef __row0row_h_
#define __row0row_h_

#include "univ.h"
#include "data0data.h"
#include "dict0types.h"
#include "trx0types.h"
#include "que0types.h"
#include "mtr0mtr.h"	
#include "rem0types.h"
#include "read0types.h"
#include "btr0types.h"

#define ROW_COPY_DATA		1
#define ROW_COPY_POINTERS	2
#define ROW_COPY_ALSO_EXTERNALS	3

/* The allowed latching order of index records is the following:
(1) a secondary index record ->
(2) the clustered index record ->
(3) rollback segment data for the clustered index record.

No new latches may be obtained while the kernel mutex is reserved.
However, the kernel mutex can be reserved while latches are owned. */


UNIV_INLINE	dulint			row_get_rec_trx_id(rec_t* rec, dict_index_t* index);

UNIV_INLINE dulint			row_get_rec_roll_ptr(rec_t* rec, dict_index_t* index);

UNIV_INLINE void			row_set_rec_trx_id(rec_t* rec, dict_index_t* index, dulint trx_id);

UNIV_INLINE void			row_set_rec_roll_ptr(rec_t* rec, dict_index_t* index, dulint roll_ptr);

dtuple_t*					row_build_index_entry(dtuple_t* row, dict_index_t* index, mem_heap_t* heap);

void						row_build_index_entry_to_tuple(dtuple_t* entry, dtuple_t* row, dict_index_t* index);

dtuple_t*					row_build(ulint type, rec_t* rec, dict_index_t* index, mem_heap_t* heap);

void						row_build_to_tuple(dtuple_t* row, dict_index_t* index, rec_t* rec);

dtuple_t*					row_rec_to_index_entry(ulint type, dict_index_t* index, rec_t* rec, mem_heap_t* heap);

dtuple_t*					row_build_row_ref(ulint type, dict_index_t* index, rec_t* rec, mem_heap_t* heap);

void						row_build_row_ref_in_tuple(dtuple_t* ref, dict_index_t* index, rec_t* rec);

void						row_build_row_ref_from_row(dtuple_t* ref, dict_table_t* table, dtuple_t* row);

UNIV_INLINE void			row_build_row_ref_fast(dtuple_t* ref, ulint* map, rec_t* rec);

ibool						row_search_on_row_ref(btr_pcur_t* pcur, ulint mode, dict_table_t* table, dtuple_t* ref, mtr_t* mtr);

rec_t*						row_get_clust_rec(ulint mode, rec_t* rec, dict_index_t* index, dict_index_t** clust_index, mtr_t* mtr);

ibool						row_search_index_entry(dict_index_t* index, dtuple_t* entry, ulint mode, btr_pcur_t* pcur, mtr_t* mtr);

#include "row0row.inl"

#endif






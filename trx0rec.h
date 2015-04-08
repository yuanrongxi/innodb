#ifndef __trx0rec_h_
#define __trx0rec_h_

#include "univ.h"
#include "trx0types.h"
#include "row0types.h"
#include "mtr0mtr.h"
#include "trx0sys.h"
#include "dict0types.h"
#include "que0types.h"
#include "data0data.h"
#include "rem0types.h"

#define TRX_UNDO_INSERT_REC				11
#define TRX_UNDO_UPD_EXIST_REC			12
#define TRX_UNDO_UPD_DEL_REC			13
#define TRX_UNDO_DEL_MARK_REC			14
#define TRX_UNDO_CMPL_INFO_MULT			16
#define TRX_UNDO_UPD_EXTERN				128

#define	TRX_UNDO_INSERT_OP				1
#define TRX_UNDO_MODIFY_OP				2


UNIV_INLINE						trx_undo_rec_t* trx_undo_rec_copy(trx_undo_rec_t* undo_rec, mem_heap_t* heap);

UNIV_INLINE ulint				trx_undo_rec_get_type(trx_undo_rec_t* undo_rec);

UNIV_INLINE ulint				trx_undo_rec_get_cmpl_info(trx_undo_rec_t* undo_rec);

UNIV_INLINE ibool				trx_undo_rec_get_extern_storage(trx_undo_rec_t* undo_rec);

UNIV_INLINE dulint				trx_undo_rec_get_undo_no(trx_undo_rec_t* undo_rec);

byte*							trx_undo_rec_get_pars(trx_undo_rec_t* undo_rec, ulint* type, ulint* cmpl_info, ibool* updated_extern,
											dulint* undo_do, dulint* table_id);

byte*							trx_undo_rec_get_row_ref(byte* ptr, dict_index_t* index, dtuple_t** ref, mem_heap_t* heap);

byte*							trx_undo_update_rec_get_sys_cols(byte* ptr, dulint* trx_id, dulint* roll_ptr, dulint* info_bits);

byte*							trx_undo_update_rec_get_update(byte* ptr, dict_index_t* index, ulint type, dulint trx_id, dulint roll_ptr,
									ulint info_bits, mem_heap_t* heap, upd_t** upd);

byte*							trx_undo_rec_get_partial_row(byte* ptr, dict_index_t* index, dtuple** row, mem_heap_t* heap);

ulint							trx_undo_report_row_operation(ulint flags, ulint op_type, que_thr_t* thr, dict_index_t* clust_entry,
									upd_t* cmpl_info, rec_t* rec, dulint* roll_ptr);

trx_undo_rec_t*					trx_undo_get_undo_rec_low(dulint roll_ptr, mem_heap_t* heap);

ulint							trx_undo_get_undo_rec(dulint roll_ptr, dulint trx_id, trx_undo_rec_t** undo_rec, mem_heap_t* heap);

ulint							trx_undo_prev_version_build(rec_t* index_rec, mtr_t* index_mtr, rec_t* rec, dict_index_t* index, 
										mem_heap_t* heap, rec_t** old_vers);

byte*							trx_undo_parse_add_undo_rec(byte* ptr, byte* end_ptr, page_t* page);

byte*							trx_undo_parse_erase_page_end(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr);

#endif






/********************************************************************
*行版本（MVCC）
*********************************************************************/

#ifndef __row0vers_h_
#define __row0vers_h_

#include "univ.h"
#include "data0data.h"
#include "dict0types.h"
#include "trx0types.h"
#include "que0types.h"
#include "rem0types.h"
#include "mtr0mtr.h"
#include "read0read.h"

/*找出插入和修改记录的二级索引的事务,事务必须在active状态*/
trx_t*			row_vers_impl_x_locked_off_kernel(rec_t* rec, dict_index_t* index);

ibool			row_vers_must_preserve_del_marked(dulint trx_id, mtr_t* mtr);

ibool			row_vers_old_has_index_entry(ibool also_curr, rec_t* rec, mtr_t* mtr, dict_index_t* index, dtuple_t* ientry);

ulint			row_vers_build_for_consistent_read(rec_t* rec, mtr_t* mtr, dict_index_t* index, read_view_t* view, 
												mem_heap_t* in_heap, rec_t** old_vers);

#endif


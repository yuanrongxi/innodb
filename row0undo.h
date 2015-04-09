#ifndef __row0undo_h_
#define __row0undo_h_

#include "univ.h"
#include "mtr0mtr.h"
#include "trx0sys.h"
#include "btr0types.h"
#include "btr0pcur.h"
#include "dict0types.h"
#include "trx0types.h"
#include "que0types.h"
#include "row0types.h"

#define	UNDO_NODE_FETCH_NEXT	1	/* we should fetch the next undo log record */
#define	UNDO_NODE_PREV_VERS		2	/* the roll ptr to previous version of a row is stored in node, and undo should be done based on it */
#define UNDO_NODE_INSERT		3
#define UNDO_NODE_MODIFY		4

undo_node_t*	row_undo_node_create(trx_t* trx, que_thr_t* parent, mem_heap_t* heap);

ibool			row_undo_search_clust_to_pcur(undo_node_t* node, que_thr_t* thr);

que_thr_t*		row_undo_step(que_thr_t* thr);

/*undo node的任务执行结构*/
struct undo_node_struct
{
	que_common_t		common;
	ulint				state;
	trx_t*				trx;
	dulint				roll_ptr;
	trx_undo_rec_t*		undo_rec;
	dulint				undo_no;
	ulint				rec_type;
	dulint				new_roll_ptr;
	dulint				new_trx_id;
	btr_pcur_t			pcur;
	dict_table_t*		table;
	ulint				cmpl_info;
	upd_t*				update;
	dtuple_t*			ref;
	dtuple_t*			row;
	dict_index_t*		index;
	mem_heap_t*			heap;
};

#endif




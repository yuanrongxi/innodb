#ifndef __row0purge_h_
#define __row0purge_h_

#include "univ.h"
#include "data0data.h"
#include "btr0types.h"
#include "btr0pcur.h"
#include "dict0types.h"
#include "trx0types.h"
#include "que0types.h"
#include "row0types.h"


struct purge_node_struct
{
	que_common_t			common;

	dulint					roll_ptr;
	trx_undo_rec_t*			undo_rec;
	trx_undo_inf_t*			reservation;

	dulint					undo_no;
	ulint					rec_type;	/*undo rec type:insert / update*/

	btr_pcur_t				pcur;
	ibool					found_clust;
	dict_table_t*			table;
	ulint					cmpl_info;
	upd_t*					update;
	dtuple_t*				ref;
	dtuple_t*				row;
	dict_index_t*			index;
	mem_heap_t*				heap;
};

/*创建一个purge que node*/
purge_node_t*	row_purge_node_create(que_thr_t* parent, mem_heap_t* heap);

/*对purge que thread的执行*/
que_thr_t*		row_purge_step(que_thr_t* thr);

#endif







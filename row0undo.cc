#include "row0undo.h"
#include "fsp0fsp.h"
#include "mach0data.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0roll.h"
#include "trx0undo.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "que0que.h"
#include "row0row.h"
#include "row0uins.h"
#include "row0umod.h"
#include "srv0srv.h"

/*创建一个undo query graph node*/
undo_node_t* row_undo_node_create(trx_t* trx, que_thr_t* parent, mem_heap_t* heap)
{
	undo_node_t* undo;
	ut_ad(trx && parent && heap);

	undo = mem_heap_alloc(heap, sizeof(undo_node_t));
	undo->common.type = QUE_NODE_UNDO;
	undo->common.parent = parent;
	undo->state = UNDO_NODE_FETCH_NEXT;
	undo->trx = trx;

	btr_pcur_init(&(undo->pcur));
	undo->heap = mem_heap_create(256);

	return undo;
}




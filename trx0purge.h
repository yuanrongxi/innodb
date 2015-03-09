#ifndef __trx0purge_h_
#define __trx0purge_h_

#include "univ.h"
#include "trx0types.h"
#include "mtr0mtr.h"
#include "trx0sys.h"
#include "que0types.h"
#include "page0page.h"
#include "usr0sess.h"
#include "fil0fil.h"

extern trx_purge_t*		purge_sys;

extern trx_undo_rec_t*	trx_purge_dummy_rec;

#define TRX_PURGE_ON			1
#define TRX_STOP_PURGE			2

struct trx_purge_struct
{
	ulint			state;				
	sess_t*			sess;
	trx_t*			trx;
	que_t*			query;
	rw_lock_t*		purge_is_running;
	rw_lock_t*		latch;
	read_view_t*	view;
	mutex_t			mutex;
	ulint			n_pages_handled;
	ulint			handle_limit;
	dulint			purge_trx_no;
	dulint			purge_undo_no;
	ibool			next_stored;
	trx_rseg_t*		rseg;
	ulint			page_no;
	ulint			offset;
	ulint			hdr_page_no;
	ulint			hdr_offset;
	trx_undo_arr_t*	arr;
	mem_heap_t*		heap;
};

UNIV_INLINE fil_addr_t			trx_purge_get_log_from_hist(fil_addr_t node_addr);

ibool							trx_purge_updage_undo_must_exist(dulint trx_id);

void							trx_purge_sys_create();

void							trx_purge_add_update_undo_to_history(trx_t* trx, pag_t* undo_page, mtr_t* mtr);

trx_undo_rec_t*					trx_purge_fetch_next_rec(dulint* roll_ptr, trx_undo_inf_t** cell, mem_heap_t* heap);

void							trx_purge_rec_release(trx_undo_inf_t* cell);

ulint							trx_purge();

void							trx_purge_sys_print();

#include "trx0purge.inl"

#endif

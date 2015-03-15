#ifndef __trx0roll_h_
#define __trx0roll_h_

#include "univ.h"
#include "trx0trx.h"
#include "trx0types.h"
#include "mtr0mtr.h"
#include "trx0sys.h"

/*Rollback node states*/
#define ROLL_NODE_SEND		1
#define ROLL_NODE_WAIT		2

extern sess_t*	trx_dummy_sess;

/*purge cell array的单元数据定义*/
struct trx_undo_inf_struct
{
	dulint			trx_no;		/*事务序号*/
	dulint			undo_no;	/*undo序号*/
	ibool			in_use;		/*是否正在purge*/
};

/*在rollback或者purge过程中保存undo rec的undo number的结构体*/
struct trx_undo_arr_struct
{
	ulint			n_cells;
	ulint			n_used;
	trx_undo_inf_t* infos;
	mem_heap_t*		heap;
};

/*在query graph中rollback命令定义*/
struct roll_node_struct
{
	que_common_t	common;
	ulint			state;
	ibool			partial;
	trx_savept_t	savept;
};

trx_savept_t				trx_savept_take(trx_t* trx);

trx_undo_arr_t*				trx_undo_arr_create();

void						trx_undo_arr_free(trx_undo_arr_t* arr);

UNIV_INLINE trx_undo_inf_t* trx_undo_arr_get_nth_info(trx_undo_arr_t* arr, ulint n);

void						trx_roll_try_truncate(trx_t* trx);

trx_undo_rec_t*				trx_roll_pop_top_rec_of_trx(trx_t* trx, dulint limit, dulint* roll_ptr, mem_heap_t* heap);

ibool						trx_undo_rec_reserve(trx_t* trx, dulint undo_no);

void						trx_undo_rec_release(trx_t* trx, dulint undo_no);

void						trx_rollback(trx_t* trx, trx_sig_t* sig, que_thr_t** next_thr);

void						trx_rollback_or_clean_all_without_sess();

void						trx_finish_rollback_off_kernel(que_t* graph, trx_t* trx, que_thr_t** next_thr);

que_t*						trx_roll_graph_build(trx_t* trx);

roll_node_t*				roll_node_create(mem_heap_t* heap);

que_thr_t*					trx_rollback_step(que_thr_t* thr);

int							trx_rollback_for_mysql(trx_t* trx);

int							trx_rollback_last_sql_stat_for_mysql(trx_t* trx);

int							trx_general_rollback_for_mysql(trx_t* trx, ibool partial, trx_savept_t* savept);

#endif





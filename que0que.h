#ifndef que0que_h_
#define que0que_h_

#include "univ.h"
#include "data0data.h"
#include "dict0types.h"
#include "trx0trx.h"
#include "srv0srv.h"
#include "usr0types.h"
#include "que0types.h"
#include "row0types.h"
#include "pars0types.h"

/* Query fork (or graph) types */
#define QUE_FORK_SELECT_NON_SCROLL	1	/* forward-only cursor */
#define QUE_FORK_SELECT_SCROLL		2	/* scrollable cursor */
#define QUE_FORK_INSERT				3
#define QUE_FORK_UPDATE				4
#define QUE_FORK_ROLLBACK			5 /* This is really the undo graph used in rollback, no signal-sending roll_node in this graph */
#define QUE_FORK_PURGE				6
#define	QUE_FORK_EXECUTE			7
#define QUE_FORK_PROCEDURE			8
#define QUE_FORK_PROCEDURE_CALL		9
#define QUE_FORK_MYSQL_INTERFACE	10
#define	QUE_FORK_RECOVERY			11

/* Query fork (or graph) states */
#define QUE_FORK_ACTIVE				1
#define QUE_FORK_COMMAND_WAIT		2
#define QUE_FORK_INVALID			3
#define QUE_FORK_BEING_FREED		4

/* Flag which is ORed to control structure statement node types */
#define QUE_NODE_CONTROL_STAT		1024

/* Query graph node types */
#define	QUE_NODE_LOCK			1
#define	QUE_NODE_INSERT			2
#define QUE_NODE_UPDATE			4
#define	QUE_NODE_CURSOR			5
#define	QUE_NODE_SELECT			6
#define	QUE_NODE_AGGREGATE		7
#define QUE_NODE_FORK			8
#define QUE_NODE_THR			9
#define QUE_NODE_UNDO			10
#define QUE_NODE_COMMIT			11
#define QUE_NODE_ROLLBACK		12
#define QUE_NODE_PURGE			13
#define QUE_NODE_CREATE_TABLE	14
#define QUE_NODE_CREATE_INDEX	15
#define QUE_NODE_SYMBOL			16
#define QUE_NODE_RES_WORD		17
#define QUE_NODE_FUNC			18
#define QUE_NODE_ORDER			19
#define QUE_NODE_PROC			(20 + QUE_NODE_CONTROL_STAT)
#define QUE_NODE_IF				(21 + QUE_NODE_CONTROL_STAT)
#define QUE_NODE_WHILE			(22 + QUE_NODE_CONTROL_STAT)
#define QUE_NODE_ASSIGNMENT		23
#define QUE_NODE_FETCH			24
#define QUE_NODE_OPEN			25
#define QUE_NODE_COL_ASSIGNMENT	26
#define QUE_NODE_FOR			(27 + QUE_NODE_CONTROL_STAT)
#define QUE_NODE_RETURN			28
#define QUE_NODE_ROW_PRINTF		29
#define QUE_NODE_ELSIF			30
#define QUE_NODE_CALL			31

/* Query thread states */
#define QUE_THR_RUNNING			1
#define QUE_THR_PROCEDURE_WAIT	2
#define	QUE_THR_COMPLETED		3	/* in selects this means that the thread is at the end of its result set
									   (or start, in case of a scroll cursor); in other statements, this means the
									   thread has done its task */
#define QUE_THR_COMMAND_WAIT	4
#define QUE_THR_LOCK_WAIT		5
#define QUE_THR_SIG_REPLY_WAIT	6
#define QUE_THR_SUSPENDED		7
#define QUE_THR_ERROR			8

/* From where the cursor position is counted */
#define QUE_CUR_NOT_DEFINED		1
#define QUE_CUR_START			2
#define	QUE_CUR_END				3

#define QUE_THR_MAGIC_N			8476583
#define QUE_THR_MAGIC_FREED		123461526

struct que_thr_struct
{
	que_common_t				common;			/*que type*/
	ulint						magic_n;		/*魔法校验值字*/

	que_node_t*					child;			/*graph child node*/
	que_t*						graph;			
	ibool						is_active;		/*是否是激活状态*/
	ulint						state;			
	que_node_t*					run_node;
	que_node_t*					prev_node;
	ulint						resource;
	UT_LIST_NODE_T(que_thr_t)	thrs;
	UT_LIST_NODE_T(que_thr_t)	trx_thrs;
	UT_LIST_NODE_T(que_thr_t)	queue;
};

struct que_fork_struct
{
	que_common_t				common;
	que_t*						graph;
	ulint						fork_type;
	ulint						n_active_thrs;
	trx_t*						trx;				/*对应的事务对象*/
	ulint						state;
	que_thr_t*					caller;
	UT_LIST_BASE_NODE_T(que_thr_t) thrs;
	sym_tab_t*					sym_table;
	ulint						id;
	ulint						command;
	ulint						param;
	ulint						cur_end;
	ulint						cur_pos;
	ibool						cur_on_row;
	dulint						n_inserts;
	dulint						n_updates;
	dulint						n_deletes;
	sel_node_t*					last_sel_node;

	UT_LIST_NODE_T(que_fork_t)	graphs;

	mem_heap_t*					heap;
};

extern ibool					que_trace_on;

void						que_graph_pushlish(que_t* graph, sess_t* sess);

que_fork_t*					que_fork_create(que_t* graph, que_node_t* parent, ulint fork_type, mem_heap_t* heap);

UNIV_INLINE que_thr_t*		que_fork_get_first_thr(que_fork_t* fork);

UNIV_INLINE que_node_t*		que_fork_get_child(que_fork_t* fork);

UNIV_INLINE void			que_node_set_parent(que_node_t*	node, que_node_t* parent);

que_thr_t*					que_thr_create(que_fork_t* parent, mem_heap_t* heap);

ibool						que_graph_try_free(que_t* graph);

void						que_graph_free_recursive(que_node_t* node);

void						que_graph_free(que_t* graph);

ibool						que_thr_stop(que_thr_t* thr);

void						que_thr_move_to_run_state_for_mysql(que_thr_t* thr, trx_t* trx);

void						que_thr_stop_for_mysql_no_error(que_thr_t* thr, trx_t* trx);

void						que_thr_stop_for_mysql(que_thr_t* thr);

void						que_run_threads(que_thr_t* thr);

void						que_fork_error_handle(trx_t* trx, que_t* fork);

void						que_thr_handle_error(que_thr_t* thr, ulint err_no, byte* err_str, ulint err_len);

void						que_thr_end_wait(que_thr_t* thr, que_thr_t** next_thr);

void						que_thr_end_wait_no_next_thr(que_thr_t* thr);

que_thr_t*					que_fork_start_command(que_fork_t* fork, ulint command, ulint param);

UNIV_INLINE trx_t*			thr_get_trx(que_thr_t* thr);

UNIV_INLINE ulint			que_node_get_type(que_node_t* node);

UNIV_INLINE dtype_t*		que_node_get_data_type(que_node_t* node);

UNIV_INLINE dfield_t*		que_node_get_val(que_node_t* node);

UNIV_INLINE ulint			que_node_get_val_buf_size(que_node_t* node);

UNIV_INLINE que_node_t*		que_node_get_next(que_node_t* node);

UNIV_INLINE que_node_t*		que_node_get_parent(que_node_t* node);

UNIV_INLINE que_node_t*		que_node_list_add_last(que_node_t* node_list, que_node_t* node);

UNIV_INLINE ulint			que_node_list_get_len(que_node_t* node_list);

UNIV_INLINE ibool			que_thr_peek_stop(que_thr_t* thr);

UNIV_INLINE ibool			que_graph_is_select(que_t* graph);

void						que_node_print_info(que_node_t* node);

#include "que0que.inl"
#endif

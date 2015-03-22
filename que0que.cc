#include "que0que.h"
#include "srv0que.h"
#include "usr0sess.h"
#include "trx0trx.h"
#include "trx0roll.h"
#include "row0undo.h"
#include "row0ins.h"
#include "row0upd.h"
#include "row0sel.h"
#include "row0purge.h"
#include "dict0crea.h"
#include "log0log.h"
#include "eval0proc.h"
#include "eval0eval.h"
#include "odbc0odbc.h"

#define QUE_PARALLELIZE_LIMIT		(64 * 256 * 256 * 256) 
#define QUE_ROUND_ROBIN_LIMIT		(64 * 256 * 256 * 256)
#define QUE_MAX_LOOPS_WITHOUT_CHECK 16

ibool que_trace_on = FALSE;
ibool que_always_false =  FALSE;


static void que_thr_move_to_run_state(que_thr_t* thr);
static que_thr_t* que_try_parallelize(que_thr_t* thr);

/*增加一个query graph到session的graphs列表中*/
void que_graph_pushlish(que_t* graph, sess_t* sess)
{
	ut_ad(mutex_own(&kernel_mutex));
	UT_LIST_ADD_LAST(graph, sess->graphs, graph);
}

/*创建一个query graph fork对象*/
que_fork_t* que_fork_create(que_t* graph, que_node_t* parent, ulint fork_type, mem_heap_t* heap)
{
	que_fork_t*	fork;

	ut_ad(heap);

	fork = mem_heap_alloc(heap, sizeof(que_fork_t));
	fork->common.type = QUE_NODE_FORK;
	fork->n_active_thrs = 0;
	
	fork->state = QUE_FORK_COMMAND_WAIT;
	if(graph != NULL)
		fork->graph = graph;
	else
		fork->graph = fork;

	fork->common.parent = parent;
	fork->fork_type = fork_type;
	fork->caller = NULL;

	UT_LIST_INIT(fork->thrs);
	fork->sym_table = NULL;

	return fork;
}

/*创建一个que_thread node对象*/
que_thr_t* que_thr_create(que_fork_t* parent, mem_heap_t* heap)
{
	que_thr_t*	thr;

	ut_ad(parent && heap);

	thr = mem_heap_alloc(heap, sizeof(que_thr_t));

	thr->common.type = QUE_NODE_THR;
	thr->common.parent = parent;

	thr->magic_n = QUE_THR_MAGIC_N;

	thr->graph = parent->graph;

	thr->state = QUE_THR_COMMAND_WAIT;

	thr->is_active = FALSE;	

	thr->run_node = NULL;
	thr->resource = 0;

	UT_LIST_ADD_LAST(thrs, parent->thrs, thr);

	return(thr);
}

/*将query thread的状态设置为QUE_THR_RUNNING,并且尝试用单个工作线程来继续执行他*/
void que_thr_end_wait(que_thr_t* thr, que_thr_t** next_thr)
{
	ibool	was_active;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(thr);
	ut_ad((thr->state == QUE_THR_LOCK_WAIT) || (thr->state == QUE_THR_PROCEDURE_WAIT) || (thr->state == QUE_THR_SIG_REPLY_WAIT));
	ut_ad(thr->run_node);

	thr->prev_node = thr->run_node;
	was_active = thr->is_active;

	/*将que_thr设置为QUE_THR_RUNNING,并且进行相对应参数的修改*/
	que_thr_move_to_run_state(thr);
	if(was_active)
		return ;

	if(next_thr != NULL && *next_thr == NULL)
		*next_thr = thr;
	else
		srv_que_task_enqueue_low(thr);
}

/*和que_thr_end_wait功能相同，如果从wait状态到active状态，不加入到que_task中*/
void que_thr_end_wait_no_next_thr(que_thr_t* thr)
{
	bool was_active;

	ut_a(thr->state == QUE_THR_LOCK_WAIT);
	ut_ad(thr);
	ut_ad((thr->state == QUE_THR_LOCK_WAIT) || (thr->state == QUE_THR_PROCEDURE_WAIT) || (thr->state == QUE_THR_SIG_REPLY_WAIT));

	was_active = thr->is_active;
	que_thr_move_to_run_state(thr);
	if(was_active)
		return ;

	/* In MySQL we let the OS thread (not just the query thread) to wait for the lock to be released: */
	srv_release_mysql_thread_if_suspended(thr); /*等待锁的释放*/
} 

/*初始化一个query thread*/
UNIV_INLINE void que_thr_init_command(que_thr_t* thr)	/* in: query thread */
{
	thr->run_node = thr;
	thr->prev_node = thr->common.parent;

	que_thr_move_to_run_state(thr);
}

/*开始执行一个query fork中的命令,优先等级：QUE_THR_COMMAND_WAIT-》QUE_THR_SUSPENDED-》QUE_THR_COMPLETED*/
que_thr_t* que_fork_start_command(que_fork_t* fork, ulint command, ulint param)
{
	que_thr_t*	thr;
	
	fork->command = command;
	fork->state = QUE_FORK_ACTIVE;
	fork->last_sel_node = NULL;

	/*先尝试启动执行一个QUE_THR_COMMAND_WAIT状态的query thread*/
	thr = UT_LIST_GET_FIRST(fork->thrs);
	while(thr != NULL){
		if(thr->state == QUE_THR_COMMAND_WAIT){
			que_thr_init_command(thr);
			return thr;
		}

		ut_ad(thr->state != QUE_THR_LOCK_WAIT);
		thr = UT_LIST_GET_NEXT(thrs, thr);
	}

	/*如果fork中没有QUE_THR_COMMAND_WAIT状态的query thread,尝试启动QUE_THR_SUSPENDED状态的query thread*/
	thr = UT_LIST_GET_FIRST(fork->thrs);
	while (thr != NULL) {
		if (thr->state == QUE_THR_SUSPENDED) {
			que_thr_move_to_run_state(thr);
			return thr;
		}

		thr = UT_LIST_GET_NEXT(thrs, thr);
	}

	/*如果连QUE_THR_SUSPENDED的query thread都没有，那么尝试执行QUE_THR_COMPLETED状态的query thread*/
	thr = UT_LIST_GET_FIRST(fork->thrs);
	while (thr != NULL) {
		if (thr->state == QUE_THR_COMPLETED) {
			que_thr_init_command(thr);
			return(thr);
		}

		thr = UT_LIST_GET_NEXT(thrs, thr);
	}

	return NULL;
}

/*事务对应的session出错了，将fork中的que_thread全部置为QUE_THR_COMPLETED,并从新启动第一个que thread*/
void que_fork_error_handle(trx_t* trx, que_t* fork)
{
	que_thr_t* thr;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(trx->sess->state == SESS_ERROR);
	ut_ad(UT_LIST_GET_LEN(trx->reply_signals) == 0);
	ut_ad(UT_LIST_GET_LEN(trx->wait_thrs) == 0);

	/*将所有que thread置为QUE_THR_COMPLETED状态*/
	thr = UT_LIST_GET_FIRST(fork->thrs);
	while(thr != NULL){
		ut_ad(!thr->is_active);
		ut_ad(thr->state != QUE_THR_SIG_REPLY_WAIT);
		ut_ad(thr->state != QUE_THR_LOCK_WAIT);

		thr->run_node = thr;
		thr->prev_node = thr->child;
		thr->state = QUE_THR_COMPLETED;

		thr = UT_LIST_GET_NEXT(thrs, thr);
	}

	/*从新将第一个que thread 设置为running状态，并放入que tasks中进行执行排队*/
	thr = UT_LIST_GET_FIRST(fork->thrs);
	que_thr_move_to_run_state(thr);
	srv_que_task_enqueue_low(thr);
}

/*检查fork中的que thrs是否全部处于state状态，如果有出去其他状态的que thread，返回FALSE*/
UNIV_INLINE ibool que_fork_all_thrs_in_state(que_fork_t* fork, ulint state)
{
	que_thr_t* thr_node;
	thr_node = UT_LIST_GET_FIRST(fork->thrs);
	while(thr_node != NULL){
		if(thr_node->state != state){
			return FALSE;
		}

		thr_node = UT_LIST_GET_NEXT(thrs, thr_node);
	}

	return TRUE;
}

/*将node后面所有的兄弟都调用que_graph_free_recursive*/
static void que_graph_free_stat_list(que_node_t* node)
{
	while(node){
		que_graph_free_recursive(node);
		node = que_node_get_next(node);
	}
}

/*释放一个que graph对象，将会递归调用que_graph_free_stat_list*/
void que_graph_free_recursive(que_node_t* node)
{
	que_fork_t*	fork;
	que_thr_t*	thr;
	undo_node_t*	undo;
	sel_node_t*	sel;
	ins_node_t*	ins;
	upd_node_t*	upd;
	tab_node_t*	cre_tab;
	ind_node_t*	cre_ind;

	if(node == NULL)
		return ;

	switch(que_node_get_type(node)){
	case QUE_NODE_FORK:
		fork = node;
		thr = UT_LIST_GET_FIRST(fork->thrs);
		while(thr){
			que_graph_free_recursive(thr);
			thr = UT_LIST_GET_NEXT(thrs, thr);
		}
		break;

	case QUE_NODE_THR:
		thr = node;

		if (thr->magic_n != QUE_THR_MAGIC_N) {
			fprintf(stderr, "que_thr struct appears corrupt; magic n %lu\n", thr->magic_n);
			mem_analyze_corruption((byte*)thr);
			ut_a(0);
		}

		thr->magic_n = QUE_THR_MAGIC_FREED;
		que_graph_free_recursive(thr->child);
		break;

	case QUE_NODE_UNDO:
		undo = node;
		mem_heap_free(undo->heap);
		break;

	case QUE_NODE_SELECT:
		sel = node;
		sel_node_free_private(sel);
		break;

	case QUE_NODE_INSERT:
		ins = node;
		que_graph_free_recursive(ins->select);
		mem_heap_free(ins->entry_sys_heap);
		break;

	case QUE_NODE_UPDATE:
		upd = node;

		if (upd->in_mysql_interface) 
			btr_pcur_free_for_mysql(upd->pcur);

		que_graph_free_recursive(upd->cascade_node);		

		if (upd->cascade_heap)
			mem_heap_free(upd->cascade_heap);

		que_graph_free_recursive(upd->select);
		mem_heap_free(upd->heap);
		break;

	case QUE_NODE_CREATE_TABLE:
		cre_tab = node;
		que_graph_free_recursive(cre_tab->tab_def);
		que_graph_free_recursive(cre_tab->col_def);
		que_graph_free_recursive(cre_tab->commit_node);

		mem_heap_free(cre_tab->heap);
		break;

	case QUE_NODE_CREATE_INDEX:
		cre_ind = node;
		que_graph_free_recursive(cre_ind->ind_def);
		que_graph_free_recursive(cre_ind->field_def);
		que_graph_free_recursive(cre_ind->commit_node);

		mem_heap_free(cre_ind->heap);
		break;

	case QUE_NODE_PROC:
		que_graph_free_stat_list(((proc_node_t*)node)->stat_list);
		break;

	case QUE_NODE_IF:
		que_graph_free_stat_list(((if_node_t*)node)->stat_list);
		que_graph_free_stat_list(((if_node_t*)node)->else_part);
		que_graph_free_stat_list(((if_node_t*)node)->elsif_list);
		break;

	case QUE_NODE_ELSIF:
		que_graph_free_stat_list(((elsif_node_t*)node)->stat_list);
		break;

	case QUE_NODE_WHILE:
		que_graph_free_stat_list(((while_node_t*)node)->stat_list);
		break;

	case QUE_NODE_FOR:
		que_graph_free_stat_list(((for_node_t*)node)->stat_list);
		break;

	case QUE_NODE_ASSIGNMENT:
	case QUE_NODE_RETURN:
	case QUE_NODE_COMMIT:
	case QUE_NODE_ROLLBACK:
	case QUE_NODE_LOCK:
	case QUE_NODE_FUNC:
	case QUE_NODE_ORDER:
	case QUE_NODE_ROW_PRINTF:
	case QUE_NODE_OPEN:
	case QUE_NODE_FETCH:
		/* No need to do anything */
		break;

	default:
		fprintf(stderr,"que_node struct appears corrupt; type %lu\n", que_node_get_type(node)); 
		mem_analyze_corruption((byte*)node);
		ut_a(0);
	}
}

/*释放一个query graph*/
void que_graph_free(que_t* graph)
{
	ut_ad(graph);

	if(graph->sym_table)
		sym_tab_free_private(graph->sym_table);
	/*递归释放各个query thread node*/
	que_graph_free_recursive(graph);

	mem_heap_free(graph->heap);
}

/*尝试释放一个query graph，并从对应的session中解除关系*/
ibool que_graph_try_free(que_t* graph)
{
	sess_t* sess;

	ut_ad(mutex_own(&kernel_mutex));
	sess = graph->trx->sess;
	
	/*检查graph是否可以free*/
	if(graph->state == QUE_FORK_BEING_FREED && graph->n_active_thrs == 0){
		UT_LIST_REMOVE(graphs, sess->graphs, graph);
		que_graph_free(graph);

		sess_try_close(sess);
		return TRUE;
	}

	return FALSE;
}

/*在SQL语句执行期间扑捉错误*/
void que_thr_handle_error(que_thr_t* thr, ulint err_no, byte* err_str, ulint err_len)
{
	UT_NOT_USED(thr);
	UT_NOT_USED(err_no);
	UT_NOT_USED(err_str);
	UT_NOT_USED(err_len);
}

static que_thr_t* que_try_parallelize(que_thr_t* thr)
{
	ut_ad(thr);
	return thr;
}

/*构建一个命令执行完成的信息到MYSQL client*/
static ulint que_build_srv_msg(byte* buf, que_fork_t* fork, sess_t* sess)
{
	ulint len;
	
	ut_ad(fork->fork_type == QUE_FORK_PROCEDURE);
	if(sess->state == SESS_ERROR)
		return 0;

	sess_srv_msg_init(sess, buf, SESS_SRV_SUCCESS);
	len = pars_proc_write_output_params_to_buf(buf + SESS_SRV_MSG_DATA, fork);

	return len;
}

/*尝试执行thr node的下一步*/
static que_thr_t* que_thr_node_step(que_thr_t* thr)
{
	ut_ad(thr->run_node == thr);

	if(thr->prev_node == thr->common.parent){ /*执行thr的孩子que thread*/
		thr->run_node = thr->child;
		return thr;
	}

	mutex_enter(&kernel_mutex);
	if(que_thr_peek_stop(thr)){ /*que thread正在等待或者有事务正在执行，不能进入完成状态*/
		mutex_exit(&kernel_mutex);
		return TRUE;
	}

	thr->state = QUE_THR_COMPLETED;
	mutex_exit(&kernel_mutex);

	return NULL;
}

/*将thr设置为QUE_THR_RUNNING状态，如果是从未激活到激活状态，进行计数器修改*/
static void que_thr_move_to_run_state(que_thr_t* thr)
{
	trx_t* trx;

	ut_ad(thr->state != QUE_THR_RUNNING);
	trx = thr_get_trx(thr);
	if(!thr->is_active){ /*从未激活到激活状态*/
		thr->graph->n_active_thrs ++;
		trx->n_active_thrs ++;
		trx->is_active = TRUE;

		ut_ad((thr->graph)->n_active_thrs == 1);
		ut_ad(trx->n_active_thrs == 1);
	}
	/*设置为执行装填*/
	thr->state = QUE_THR_RUNNING;
}

/*对que thread的引用计数进行递减，可能会启动对应事务新的sig handling*/
static void que_thr_dec_refer_count(que_thr_t* que_thr_t** next_thr)
{
	que_fork_t*	fork;
	trx_t*		trx;
	sess_t*		sess;
	ibool		send_srv_msg		= FALSE;
	ibool		release_stored_proc	= FALSE;
	ulint		msg_len			= 0;
	byte		msg_buf[ODBC_DATAGRAM_SIZE];
	ulint		fork_type;
	ibool		stopped;

	fork = thr->common.parent;
	trx = thr->graph->trx;
	sess = trx->sess;

	mutex_enter(&kernel_mutex);
	
	ut_ad(thr->is_active);
	if(thr->state == QUE_THR_RUNNING){
		stopped = que_thr_stop(thr); /*停止que thread*/
		if(!stopped){ /*不能进行停止，可能有等待或者正在执行的事务*/
			if(next_thr && **next_thr == NULL)
				*next_thr = thr;
			else /*重新进入工作线程排队*/
				srv_que_task_enqueue_low(thr);

			mutex_exit(&kernel_mutex);
			return ;
		}
	}

	ut_ad(fork->n_active_thrs == 1);
	ut_ad(trx->n_active_thrs == 1);

	fork->n_active_thrs --;
	trx->n_active_thrs --;
	thr->is_active = FALSE;

	if(trx->n_active_thrs > 0){
		mutex_exit(&kernel_mutex);
		return;
	}

	fork_type = fork->fork_type;
	/*如果que graph的所有que thr都处于QUE_THR_COMPLETED状态，*/
	if(que_fork_all_thrs_in_state(fork, QUE_THR_COMPLETED)){
		if(fork_type == QUE_FORK_ROLLBACK){ /*回滚类型*/
			ut_ad(UT_LIST_GET_LEN(trx->signals) > 0);
			ut_ad(trx->handling_signals == TRUE);
			/*完成一个事务的回滚操作*/
			trx_finish_rollback_off_kernel(fork, trx, next_thr);
		}
		else if(fork_type == QUE_FORK_PURGE){

		}
		else if(fork_type == QUE_FORK_RECOVERY){

		}
		else if(fork_type == QUE_FORK_MYSQL_INTERFACE){

		}
		else if(fork->common.parent == NULL && fork->caller == NULL && UT_LIST_GET_LEN(trx->signals) == 0){
			ut_a(0);

			/*构建一个回应msyql client的消息*/
			fork->state = QUE_FORK_COMMAND_WAIT;
			msg_len = que_build_srv_msg(msg_buf, fork, sess);
			send_srv_msg = TRUE;
			if(fork->fork_type == QUE_FORK_PROCEDURE)
				release_stored_proc = TRUE;

			ut_ad(trx->graph == fork);
			trx->graph = NULL;
		}
		else
			ut_a(0);
	}

	/*事务中还有执行信号在排队，继续启动一个新的sig让que thread来执行*/
	if(UT_LIST_GET_LEN(trx->signals) > 0 && trx->n_active_thrs == 0){
		ut_ad(!send_srv_msg);
		trx_sig_start_handle(trx, next_thr);
	}

	if(trx->handling_signals && UT_LIST_GET_LEN(trx->signals) == 0)
		trx_end_signal_handling(trx);

	mutex_exit(&kernel_mutex);

	if(send_srv_msg){
		sess_command_completed_message(sess, msg_buf, msg_len);
	}

	if(release_stored_proc) /*返回存储过程的处理结果*/
		dict_procedure_release_parsed_copy(fork);
}

/*停止一个处于事务执行状态的query thread,如果成功，返回TRUE*/
ibool que_thr_stop(que_thr_t* thr)
{
	trx_t*	trx;
	que_t*	graph;
	ibool	ret	= TRUE;

	ut_ad(mutex_own(&kernel_mutex));

	graph = thr->graph;
	trx = graph->trx;

	if(graph->state == QUE_FORK_COMMAND_WAIT)
		thr->state = QUE_THR_SUSPENDED;
	else if(trx->que_state == TRX_QUE_LOCK_WAIT){ /*事务处于锁等待状态*/
		UT_LIST_ADD_FIRST(trx_thrs, trx->wait_thrs, thr);
		thr->state = QUE_THR_LOCK_WAIT;
	}
	else if(trx->error_state != DB_SUCCESS && trx->error_state != DB_LOCK_WAIT) /*事务出错误状态*/
		thr->state = QUE_THR_COMPLETED;
	else if(UT_LIST_GET_LEN(trx->signals) > 0 && graph->fork_type != QUE_FORK_ROLLBACK) /*trx还有信号为处理，只能处于暂停状态*/
		thr->state = QUE_THR_SUSPENDED;
	else{
		ut_ad(graph->state == QUE_FORK_ACTIVE);
		ret = FALSE;
	}

	return ret;
}

/*为mysql做的一个que thread stop函数接口，用来停止一个dummy query thread*/
void que_thr_stop_for_mysql(que_thr_t* thr)
{
	ibool	stopped 	= FALSE;
	trx_t*	trx;

	trx = trx_get_trx(thr);
	mutex_enter(&kernel_mutex);

	if(thr->state == QUE_THR_RUNNING){
		if(trx->error_state != DB_SUCCESS && trx->error_state != DB_LOCK_WAIT){
			thr->state = QUE_THR_COMPLETED;
			stopped = TRUE;
		}

		if(!stopped){
			mutex_exit(&kernel_mutex);
			return ;
		}
	}

	thr->is_active = FALSE;
	(thr->graph)->n_active_thrs--;

	trx->n_active_thrs--;

	mutex_exit(&kernel_mutex);
}

/**************************************************************************
Moves a thread from another state to the QUE_THR_RUNNING state. Increments
the n_active_thrs counters of the query graph and transaction if thr was
not active. */
void que_thr_move_to_run_state_for_mysql(que_thr_t*	thr, trx_t*	trx)	
{
	if (thr->magic_n != QUE_THR_MAGIC_N) {
		fprintf(stderr, "que_thr struct appears corrupt; magic n %lu\n", thr->magic_n);
		mem_analyze_corruption((byte*)thr);

		ut_a(0);
	}

	if (!thr->is_active) {
		thr->graph->n_active_thrs++;
		trx->n_active_thrs++;
		thr->is_active = TRUE;
	}

	thr->state = QUE_THR_RUNNING;
}

/*为mysql提供一个停止que thread的接口*/
void que_thr_stop_for_mysql_no_error(que_thr_t*	thr, trx_t*	trx)	/* in: transaction */
{
	ut_ad(thr->state == QUE_THR_RUNNING);

	if (thr->magic_n != QUE_THR_MAGIC_N) {
		fprintf(stderr, "que_thr struct appears corrupt; magic n %lu\n", thr->magic_n);
		mem_analyze_corruption((byte*)thr);

		ut_a(0);
	}

	thr->state = QUE_THR_COMPLETED;
	thr->is_active = FALSE;
	(thr->graph)->n_active_thrs--;

	trx->n_active_thrs--;
}

/**************************************************************************
Prints info of an SQL query graph node. */

void que_node_print_info(que_node_t* node)	
{
	ulint	type;
	char*	str;
	ulint	addr;

	type = que_node_get_type(node);

	addr = (ulint)node;
	if (type == QUE_NODE_SELECT) {
		str = "SELECT";
	} else if (type == QUE_NODE_INSERT) {
		str = "INSERT";
	} else if (type == QUE_NODE_UPDATE) {
		str = "UPDATE";
	} else if (type == QUE_NODE_WHILE) {
		str = "WHILE";
	} else if (type == QUE_NODE_ASSIGNMENT) {
		str = "ASSIGNMENT";
	} else if (type == QUE_NODE_IF) {
		str = "IF";
	} else if (type == QUE_NODE_FETCH) {
		str = "FETCH";
	} else if (type == QUE_NODE_OPEN) {
		str = "OPEN";
	} else if (type == QUE_NODE_PROC) {
		str = "STORED PROCEDURE";
	} else if (type == QUE_NODE_FUNC) {
		str = "FUNCTION";
	} else if (type == QUE_NODE_LOCK) {
		str = "LOCK";
	} else if (type == QUE_NODE_THR) {
		str = "QUERY THREAD";
	} else if (type == QUE_NODE_COMMIT) {
		str = "COMMIT";
	} else if (type == QUE_NODE_UNDO) {
		str = "UNDO ROW";
	} else if (type == QUE_NODE_PURGE) {
		str = "PURGE ROW";
	} else if (type == QUE_NODE_ROLLBACK) {
		str = "ROLLBACK";
	} else if (type == QUE_NODE_CREATE_TABLE) {
		str = "CREATE TABLE";
	} else if (type == QUE_NODE_CREATE_INDEX) {
		str = "CREATE INDEX";
	} else if (type == QUE_NODE_FOR) {
		str = "FOR LOOP";
	} else if (type == QUE_NODE_RETURN) {
		str = "RETURN";
	} else {
		str = "UNKNOWN NODE TYPE";
	}

	printf("Node type %lu: %s, address %lx\n", type, str, addr);
}
/**************************************************************************
Performs an execution step on a query thread. */
UNIV_INLINE que_thr_t* que_thr_step(que_thr_t*	thr)	/* in: query thread */
{
	que_node_t*	node;
	que_thr_t*	old_thr;
	trx_t*		trx;
	ulint		type;
	
	ut_ad(thr->state == QUE_THR_RUNNING);

	thr->resource++;
	
	type = que_node_get_type(thr->run_node);
	node = thr->run_node;

	old_thr = thr;
	
	if (type & QUE_NODE_CONTROL_STAT) {
		if ((thr->prev_node != que_node_get_parent(node)) && que_node_get_next(thr->prev_node)) {
			/* The control statements, like WHILE, always pass the
			control to the next child statement if there is any
			child left */
			thr->run_node = que_node_get_next(thr->prev_node);
		} else if (type == QUE_NODE_IF) {
			if_step(thr);
		} else if (type == QUE_NODE_FOR) {
			for_step(thr);
		} else if (type == QUE_NODE_PROC) {
			/* We can access trx->undo_no without reserving
			trx->undo_mutex, because there cannot be active query
			threads doing updating or inserting at the moment! */	
			if (thr->prev_node == que_node_get_parent(node)) {
				trx = thr_get_trx(thr);
				trx->last_sql_stat_start.least_undo_no = trx->undo_no;
			}
			
			proc_step(thr);
		} else if (type == QUE_NODE_WHILE) {
			while_step(thr);
		}
	} else if (type == QUE_NODE_ASSIGNMENT) {
		assign_step(thr);
	} else if (type == QUE_NODE_SELECT) {
		thr = row_sel_step(thr);
	} else if (type == QUE_NODE_INSERT) {
		thr = row_ins_step(thr);
	} else if (type == QUE_NODE_UPDATE) {
		thr = row_upd_step(thr);
	} else if (type == QUE_NODE_FETCH) {
		thr = fetch_step(thr);
	} else if (type == QUE_NODE_OPEN) {
		thr = open_step(thr);
	} else if (type == QUE_NODE_FUNC) {
		proc_eval_step(thr);
	} else if (type == QUE_NODE_LOCK) {
		ut_error;
	} else if (type == QUE_NODE_THR) {
		thr = que_thr_node_step(thr);
	} else if (type == QUE_NODE_COMMIT) {
		thr = trx_commit_step(thr);
	} else if (type == QUE_NODE_UNDO) {
		thr = row_undo_step(thr);
	} else if (type == QUE_NODE_PURGE) {
		thr = row_purge_step(thr);
	} else if (type == QUE_NODE_RETURN) {
		thr = return_step(thr);
	} else if (type == QUE_NODE_ROLLBACK) {
		thr = trx_rollback_step(thr);
	} else if (type == QUE_NODE_CREATE_TABLE) {
		thr = dict_create_table_step(thr);
	} else if (type == QUE_NODE_CREATE_INDEX) {
		thr = dict_create_index_step(thr);
	} else if (type == QUE_NODE_ROW_PRINTF) {
		thr = row_printf_step(thr);
	} else {
		ut_error;
	}

	old_thr->prev_node = node;

	return thr;
}

que_thr_t* que_thr_check_if_switch(que_thr_t* thr, ulint* cumul_reource)
{
	que_thr_t*	next_thr;
	ibool		stopped;

	if(que_thr_peek_stop(thr)){ /*可以对thr进行停止*/
		mutex_enter(&kernel_mutex);
		stopped = que_thr_stop(thr);
		mutex_exit(&kernel_mutex);

		if(stopped){
			next_thr = NULL;
			que_thr_dec_refer_count(thr, &next_thr);

			if(next_thr == NULL)
				return NULL;

			thr = next_thr;
		}
	}

	if(thr->resource > QUE_PARALLELIZE_LIMIT){
		thr = que_try_parallelize(thr);
		thr->resource = 0;
	}

	(*cumul_resource)++;
	/*server task queue用轮询方式执行query thread*/
	if(*cumul_reource > QUE_ROUND_ROBIN_LIMIT){
		if(srv_get_thread_type() == SRV_COM){
			ut_ad(thr->is_active);
			srv_que_task_enqueue(thr);
			return NULL;
		}
		else
			thr = srv_que_round_robin(thr);
		*curmul_resource = 0;
	}

	return thr;
}

void que_run_threads(que_thr_t* thr)
{
	que_thr_t*	next_thr;
	ulint		cumul_resource;	
	ulint		loop_count;

	ut_ad(thr->state == QUE_THR_RUNNING);
	ut_ad(!mutex_own(&kernel_mutex));

	loop_count = QUE_MAX_LOOPS_WITHOUT_CHECK;
	cumul_resource = 0;

loop:
	if(loop_count >= QUE_MAX_LOOPS_WITHOUT_CHECK){
	}

	log_free_check();

	/*对que thread的执行*/
	next_thr = que_thr_step(thr);
	ut_ad(sync_thread_levels_empty_gen(TRUE));

	loop_count ++;
	if(next_thr != thr){
		que_thr_dec_refer_count(thr, &next_thr);
		if(next_thr == NULL)
			return ;

		loop_count = QUE_MAX_LOOPS_WITHOUT_CHECK;
		thr = next_thr;
	}
	goto loop;
}

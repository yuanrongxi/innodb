#include "trx0trx.h"

#include "trx0undo.h"
#include "trx0rseg.h"
#include "log0log.h"
#include "que0que.h"
#include "lock0lock.h"
#include "trx0roll.h"
#include "usr0sess.h"
#include "read0read.h"
#include "srv0srv.h"
#include "thr0loc.h"
#include "btr0sea.h"

void innobase_mysql_print_thd(char* buf, void* thd);

sess_t*		trx_dummy_sess = NULL;

/*当前为mysql分配的事务数*/
ulint		trx_n_mysql_transactions = 0;

/*和trx_start_if_not_started功能一样*/
void trx_start_if_not_started_noninline(trx_t* trx)
{
	trx_start_if_not_started(trx);
}

/*键值重复错误信息*/
void* trx_get_error_info(trx_t* trx)
{
	return trx->error_info;
}

trx_t* trx_create(sess_t* sess)
{
	trx_t* trx; 

	ut_ad(mutex_own(&kernel_mutex));

	trx = mem_alloc(sizeof(trx_t));
	trx->magic_n = TRX_MAGIC_N;
	trx->op_info = "";
	trx->type = TRX_USER;

	trx->conc_state = TRX_NOT_STARTED;
	trx->start_time = time(NULL);

	trx->check_foreigns = TRUE;
	trx->check_unique_secondary = TRUE;
	trx->dict_operation = FALSE;			/*DDL操作标志*/

	trx->mysql_thd = NULL;

	trx->n_mysql_tables_in_use = 0;
	trx->mysql_n_tables_locked = 0;

	trx->mysql_log_file_name = NULL;
	trx->mysql_log_offset = 0;
	trx->mysql_master_log_file_name = "";
	trx->mysql_master_log_pos = 0;

	trx->ignore_duplicates_in_insert = FALSE;

	mutex_create(&(trx->undo_mutex));
	mutex_set_level(&(trx->undo_mutex), SYNC_TRX_UNDO);

	trx->rseg = NULL;
	trx->undo_no = ut_dulint_zero;
	trx->last_sql_stat_start.least_undo_no = ut_dulint_zero;
	trx->insert_undo = NULL;
	trx->update_undo = NULL;
	trx->undo_no_arr = NULL;

	trx->error_state = DB_SUCCESS;
	trx->sess = sess;
	trx->que_state = TRX_QUE_RUNNING;
	trx->n_active_thrs = 0;

	trx->handling_signals = FALSE;

	UT_LIST_INIT(trx->signals);
	UT_LIST_INIT(trx->reply_signals);

	trx->graph = NULL;

	trx->wait_lock = NULL;
	UT_LIST_INIT(trx->wait_thrs);

	/*事务的锁对应的堆是在buffer pool中建立的*/
	trx->lock_heap = mem_heap_create_in_buffer(256);
	UT_LIST_INIT(trx->trx_locks);

	trx->has_dict_foreign_key_check_lock = FALSE;
	trx->has_search_latch = FALSE;
	trx->search_latch_timeout = BTR_SEA_TIMEOUT;

	trx->declared_to_be_inside_innodb = FALSE;
	trx->n_tickets_to_enter_innodb = 0;

	trx->auto_inc_lock = NULL;

	/*建立一个read view堆*/
	trx->read_view_heap = mem_heap_create(256);
	trx->read_view = NULL;

	return trx;
}

/*为上层mysql创建一个事务对象*/
trx_t* trx_allocate_for_mysql()
{
	trx_t* trx;
	
	mutex_enter(&kernel_mutex);

	if(!trx_dummy_sess)
		trx_dummy_sess = sess_open(NULL, (byte*)"Dummy sess", ut_strlen("Dummy sess"));

	trx = trx_create(trx_dummy_sess);
	
	/*将事务对象加入到trx_sys的队列中*/
	trx_n_mysql_transactions ++;
	UT_LIST_ADD_FIRST(mysql_trx_list, trx_sys->mysql_trx_list, trx);

	mutex_exit(&kernel_mutex);

	trx->mysql_thread_id = os_thread_get_curr_id();

	return trx;
}

/*为innodb自己创建一个后台处理的事务*/
trx_t* trx_allocate_for_background()
{
	trx_t* trx;

	mutex_enter(&kernel_mutex);

	if(!trx_dummy_sess)
		trx_dummy_sess = sess_open(NULL, (byte*)"Dummy sess", ut_strlen("Dummy sess"));

	trx = trx_create(trx_dummy_sess);

	mutex_exit(&kernel_mutex);

	return trx;
}

/*释放hash search latch,这个latch在btree search时被lock*/
void trx_search_latch_release_if_reserved(trx_t* trx)
{
	if(trx->has_search_latch){
		rw_lock_s_unlock(&btr_search_latch);
		trx->has_search_latch = FALSE;
	}
}

/*撤销一个事务对象*/
void trx_free(trx_t* trx)
{
	ut_ad(mutex_own(&kernel_mutex));

	ut_a(trx->magic_n == TRX_MAGIC_N);

	/*首先将magic的值设置为*/
	trx->magic_n = 11112222;

	ut_a(trx->conc_state == TRX_NOT_STARTED);

	mutex_free(&(trx->undo_mutex));

	ut_a(trx->insert_undo == NULL); 
	ut_a(trx->update_undo == NULL); 

	ut_a(trx->n_mysql_tables_in_use == 0);
	ut_a(trx->mysql_n_tables_locked == 0);

	if(trx->undo_no_arr)
		trx_undo_arr_free(trx->undo_no_arr);

	ut_a(UT_LIST_GET_LEN(trx->signals) == 0);
	ut_a(UT_LIST_GET_LEN(trx->reply_signals) == 0);

	ut_a(trx->wait_lock == NULL);
	ut_a(UT_LIST_GET_LEN(trx->wait_thrs) == 0);

	ut_a(!trx->has_search_latch);
	ut_a(!trx->auto_inc_lock);

	if(trx->lock_heap)
		mem_heap_free(trx->lock_heap);

	ut_a(UT_LIST_GET_LEN(trx->trx_locks) == 0);

	if(trx->read_view_heap)
		mem_heap_free(trx->read_view_heap);

	ut_a(trx->read_view_heap == NULL);

	mem_free(trx);
}

/*释放一个为上层MYSQL分配的事务对象*/
void trx_free_for_mysql(trx_t* trx)
{
	/*将事务对应的线程对象从线程管理的hash table删除*/
	thr_local_free(trx->mysql_thread_id);

	/*从trx_sys事务队列中删除*/
	mutex_enter(&kernel_mutex);
	UT_LIST_REMOVE(mysql_trx_list, trx_sys->mysql_trx_list, trx);

	trx_free(trx);

	ut_a(trx_n_mysql_transactions > 0);
	trx_n_mysql_transactions --;

	mutex_exit(&kernel_mutex);
}

void trx_free_for_background(trx_t* trx)
{
	mutex_enter(&kernel_mutex);
	trx_free(trx);
	mutex_exit(&kernel_mutex);
}

/*将trx按由小到大插入到trx_sys的trx_list当中*/
static void trx_list_insert_ordered(trx_t* trx)
{
	trx_t* trx2;

	ut_ad(mutex_own(&kernel_mutex));

	/*找到trx插入的位置*/
	trx2 = UT_LIST_GET_FIRST(trx_sys->trx_list);
	while(trx2 != NULL){
		if(ut_dulint_cmp(trx->id, trx2->id) >= 0){
			ut_ad(ut_dulint_cmp(trx->id, trx2->id) == 1);
			break;
		}

		trx2 = UT_LIST_GET_NEXT(trx_list, trx2);
	}

	if(trx2 != NULL){
		/*找到trx2前一个位置*/
		trx2 = UT_LIST_GET_PREV(trx_list, trx2);

		if(trx2 == NULL){
			UT_LIST_ADD_FIRST(trx_list, trx_sys->trx_list, trx);
		}
		else{
			UT_LIST_INSERT_AFTER(trx_list, trx_sys->trx_list, trx2, trx);
		}
	}
	else
		UT_LIST_ADD_FIRST(trx_list, trx_sys->trx_list, trx);
}

/*在innodb刚开始启动时，初始化trx_sys->trx_list,通过rseg_list来做恢复*/
void trx_lists_init_at_db_start()
{
	trx_rseg_t* rseg;
	trx_undo_t* undo;
	trx_t*		trx;

	UT_LIST_INIT(trx_sys->trx_list);
	
	rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);
	while(rseg != NULL){
		/*将insert undo list中的事务进行初始化*/
		undo = UT_LIST_GET_FIRST(rseg->insert_undo_list);
		while(undo != NULL){
			trx = trx_create(NULL);
			trx->id = undo->trx_id;
			trx->insert_undo = undo;
			trx->rseg = rseg;

			if(undo->state != TRX_UNDO_ACTIVE){
				trx->conc_state = TRX_COMMITTED_IN_MEMORY;
				trx->no = trx->id;
			}
			else{
				trx->conc_state = TRX_ACTIVE;
				trx->no = ut_dulint_max;
			}

			if(undo->dict_operation){
				trx->dict_operation = undo->dict_operation;
				trx->table_id = undo->table_id;
			}

			if(!undo->empty)
				trx->undo_no = ut_dulint_add(undo->top_undo_no, 1);

			trx_list_insert_ordered(trx);
			undo = UT_LIST_GET_NEXT(undo_list, undo);
		}

		/*将undo update list中的事务初始化*/
		undo = UT_LIST_GET_FIRST(rseg->update_undo_list);
		while(undo != NULL){
			trx = trx_get_on_id(undo->trx_id);
			if(trx == NULL){
				trx = trx_create(NULL);
				trx->id = undo->trx_id;
				if(undo->state != TRX_UNDO_ACTIVE){
					trx->conc_state = TRX_COMMITTED_IN_MEMORY;
					trx->no = trx->id;
				}
				else{
					trx->conc_state = TRX_ACTIVE;
					trx_no = ut_dulnt_max;
				}

				trx->rseg = rseg;
				trx_list_insert_ordered(trx);

				/*设置DDL信息*/
				if (undo->dict_operation){
					trx->dict_operation = undo->dict_operation;
					trx->table_id = undo->table_id;
				}
			}

			trx->update_undo = undo;
			if(!undo->empty && ut_dulint_cmp(undo->top_undo_no, trx->undo_no) >= 0){
				trx->undo_no = ut_dulint_add(undo->top_undo_no, 1);
			}

			undo = UT_LIST_GET_NEXT(undo_list, undo);
		}
		rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
	}
}

/*为事务分配一个roll segment，以轮询方式轮询trx_sys->rseg_list,从上一次分配的roll segment下一个node开始，
会跳过TRX_SYS_SYSTEM_RSEG_ID的roll segment*/
UNIV_INLINE ulint trx_assign_rseg()
{
	trx_rseg_t* rseg = trx_sys->latest_rseg;
	ut_ad(mutex_own(&kernel_mutex));

loop:
	rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
	if(rseg == NULL)
		rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);

	if(rseg->id == TRX_SYS_SYSTEM_RSEG_ID && UT_LIST_GET_LEN(trx_sys->rseg_list) > 1)
		goto loop;

	trx_sys->latest_rseg = rseg;

	return rseg->id;
}

/*启动一个事务*/
ibool trx_start_low(trx_t* trx, ulint rseg_id)
{
	trx_rseg_t*	rseg;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(trx->rseg == NULL);
	
	/*如果是PURGE trx，直接激活就可以*/
	if(trx->type == TRX_PURGE){
		trx->id = ut_ulint_zero;
		trx->conc_state = TRX_ACTIVE;
		trx->start_time = time(NULL);

		return TRUE;
	}

	ut_ad(trx->conc_state != TRX_ACTIVE);
	/*为事务分配一个roll segment*/
	if(rseg_id == ULINT_UNDEFINED)
		rseg_id = trx_assign_rseg();
	rseg = trx_sys_get_nth_rseg(trx_sys, rseg_id);

	trx_id = trx_sys_get_new_trx_id();
	trx->no = ut_dulint_max;
	trx->rseg = rseg;

	trx->conc_state = TRX_ACTIVE;
	trx->start_time = time(NULL);

	/*将启动的事务加入到trx_sys的trx_list的头上*/
	UT_LIST_ADD_FIRST(trx_list, trx_sys->trx_list, trx);

	return TRUE;
}

/*启动事务*/
ibool trx_start(trx_t* trx, ulint rseg_id)
{
	ibool ret;

	mutex_enter(&kernel_mutex);
	ret = trx_start_low(trx, rseg_id);
	mutex_exit(&kernel_mutex);

	return ret;
}

/*提交一个事务*/
void trx_commit_off_kernel(trx_t* trx)
{
	page_t*		update_hdr_page;
	dulint		lsn;
	trx_rseg_t*	rseg;
	trx_undo_t*	undo;
	ibool		must_flush_log	= FALSE;
	mtr_t		mtr;

	ut_ad(mutex_own(&kernel_mutex));

	rseg = trx->rseg;
	if(trx->insert_undo != NULL && trx->update_undo != NULL){ /*事务修改了记录和page，必须flush log*/
		mutex_exit(&kernel_mutex);

		mtr_start(&mtr);
		/*有修改或者新增产生，必须flush redo log*/
		must_flush_log = TRUE;

		mutex_enter(&(rseg->mutex));
		/*对insert undo的finish*/
		if(trx->insert_undo != NULL)
			trx_undo_set_state_at_finish(trx, trx->insert_undo, &mtr);

		/*对update undo的finish*/
		undo = trx->update_undo;
		if(undo != NULL){
			mutex_enter(&kernel_mutex);
			trx->no = trx_sys_get_new_trx_no();
			mutex_exit(&kernel_mutex);

			update_hdr_page = trx_undo_set_state_at_finish(trx, undo, &mtr);

			trx_undo_update_cleanup(trx, update_hdr_page, &mtr);
		}

		mutex_exit(&(reg->mutex));

		/*对上层mysql binlog的修改*/
		if (trx->mysql_log_file_name) {
			trx_sys_update_mysql_binlog_offset(trx->mysql_log_file_name, trx->mysql_log_offset, TRX_SYS_MYSQL_LOG_INFO, &mtr);
			trx->mysql_log_file_name = NULL;
		}

		if (trx->mysql_master_log_file_name[0] != '\0') {
			/* This database server is a MySQL replication slave */ 
			trx_sys_update_mysql_binlog_offset(trx->mysql_master_log_file_name,
				trx->mysql_master_log_pos, TRX_SYS_MYSQL_MASTER_LOG_INFO, &mtr);
		}

		/*确定本事务最后的LSN，用于FLUSH LOG to disk*/
		mtr_commit(&mtr);
		lsn = mtr.end_lsn;

		mutex_enter(&kernel_mutex);
	}

	ut_ad(trx->conc_state == TRX_ACTIVE);
	ut_ad(mutex_own(&kernel_mutex));

	trx->conc_state = TRX_COMMITTED_IN_MEMORY;

	/*释放本事务持有的锁*/
	lock_release_off_kernel(trx);

	if (trx->read_view) {
		read_view_close(trx->read_view);
		mem_heap_empty(trx->read_view_heap);
		trx->read_view = NULL;
	}

	if(must_flush_log){
		mutex_exit(&kernel_mutex);

		if(trx->insert_undo != NULL)
			trx_undo_insert_cleanup(trx);

		/*将本事务的redo log全部flush到磁盘, force-log-commit规则*/
		if(srv_flush_log_at_trx_commit)
			log_flush_up_to(lsn, LOG_WAIT_ONE_GROUP);

		mutex_enter(&kernel_mutex);
	}

	trx->conc_state = TRX_NOT_STARTED;
	trx->rseg = NULL;
	trx->undo_no = ut_dulint_zero;
	trx->last_sql_stat_start.least_undo_no = ut_dulint_zero;

	ut_ad(UT_LIST_GET_LEN(trx->wait_thrs) == 0);
	ut_ad(UT_LIST_GET_LEN(trx->trx_locks) == 0);

	/*从正在执行的trx_list队列中删除*/
	UT_LIST_REMOVE(trx_list, trx_sys->trx_list, trx);
}

/*清除一个正在执行的事务*/
void trx_cleanup_at_db_startup(trx_t* trx)
{
	if (trx->insert_undo != NULL)
		trx_undo_insert_cleanup(trx);

	trx->conc_state = TRX_NOT_STARTED;
	trx->rseg = NULL;
	trx->undo_no = ut_dulint_zero;
	trx->last_sql_stat_start.least_undo_no = ut_dulint_zero;

	UT_LIST_REMOVE(trx_list, trx_sys->trx_list, trx);
}

/*分配一个读视图，一般在一个新事务开始的时候调用这个函数*/
read_view_t* trx_assign_read_view(trx_t* trx)
{
	ut_ad(trx->conc_state == TRX_ACTIVE);

	if(trx->read_view)
		return trx->read_view;

	mutex_enter(&kernel_mutex);

	if(!trx->read_view)
		trx->read_view = read_view_open_now(trx, trx-read_view_heap);

	mutex_exit(&kernel_mutex);
}

/*提交一个事务，并将事务相关的TRX_SIG_COMMIT删除*/
static void trx_handle_commit_sig_off_kernel(trx_t* trx, que_thr_t* next_thr)
{
	trx_sig_t*	sig;
	trx_sig_t*	next_sig;

	ut_ad(mutex_own(&kernel_mutex));

	trx->que_state = TRX_QUE_COMMITTING;

	trx_commit_off_kernel(trx);
	ut_ad(UT_LIST_GET_LEN(trx->wait_thrs) == 0);

	/*删除所有的TRX_SIG_COMMIT信号*/
	sig = UT_LIST_GET_FIRST(trx->signals);
	while(sig != NULL){
		next_sig = UT_LIST_GET_NEXT(trx->signals, sig);
		if(sig->type == TRX_SIG_COMMIT){
			trx_sig_reply(trx, sig, next_thr);
			trx_sig_remove(trx, sig);
		}
		sig = next_sig;
	}
	trx->que_state = TRX_QUE_RUNNING;
}
/*释放所有因为本事务等待的线程，结束他们的等待*/
void trx_end_lock_wait(trx_t* trx)
{
	que_thr_t*	thr;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(trx->que_state == TRX_QUE_LOCK_WAIT);

	thr = UT_LIST_GET_FIRST(trx->wait_thrs);

	while(thr != NULL){
		que_thr_end_wait_no_next_thr(thr);
		UT_LIST_REMOVE(trx_thrs, trx->wait_thrs, thr);
		thr = UT_LIST_GET_FIRST(trx->wait_thrs);
	}

	trx->que_state = TRX_QUE_RUNNING;
}

/*将trx所有lock wait threads移出队列，并且将他们设置成挂起（SUSPENDED）状态*/
static void trx_lock_wait_to_suspended(trx_t* trx)
{
	que_thr_t*	thr;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(trx->que_state == TRX_QUE_LOCK_WAIT);

	thr = UT_LIST_GET_FIRST(trx->wait_thrs);

	while (thr != NULL){
		thr->state = QUE_THR_SUSPENDED;
		UT_LIST_REMOVE(trx_thrs, trx->wait_thrs, thr);
		thr = UT_LIST_GET_FIRST(trx->wait_thrs);
	}

	trx->que_state = TRX_QUE_RUNNING;
}

/*将trx中的reply_signals的线程设置为挂起状态*/
static void trx_sig_reply_wait_to_suspended(trx_t* trx)
{
	trx_sig_t*	sig;
	que_thr_t*	thr;

	ut_ad(mutex_own(&kernel_mutex));

	sig = UT_LIST_GET_FIRST(trx->reply_signals);
	while(sig != NULL){
		thr = sig->receiver;
		ut_ad(thr->state == QUE_THR_SIG_REPLY_WAIT);

		thr->state = QUE_THR_SUSPENDED;
		sig->receiver = NULL;
		sig->reply = FALSE;

		UT_LIST_REMOVE(reply_signals, trx->reply_signals, sig);
		sig = UT_LIST_GET_FIRST(trx->reply_signals);
	}
}

/*判断事务signal的兼容性*/
static ibool trx_sig_is_compatible(trx_t* trx, ulint type, ulint sender)
{
	trx_sig_t* sig;

	ut_ad(mutex_own(&kernel_mutex));

	if(UT_LIST_GET_LEN(trx->signals) == 0)
		return TRUE;

	if(sender == TRX_SIG_SELF){
		if(type == TRX_SIG_ERROR_OCCURRED)
			return TRUE;
		else if(type == TRX_SIG_BREAK_EXECUTION)
			return TRUE;
		else
			return FALSE;
	}

	ut_ad(sender == TRX_SIG_OTHER_SESS);

	sig = UT_LIST_GET_FIRST(trx->signals);
	if(type == TRX_SIG_COMMIT){ /*提交信号，不兼容rollback*/
		while(sig != NULL){
			if(sig->type == TRX_SIG_TOTAL_ROLLBACK)
				return FALSE;

			sig = UT_LIST_GET_NEXT(signals, sig);
		}

		return TRUE;
	}
	else if(type == TRX_SIG_TOTAL_ROLLBACK){
		while (sig != NULL) {
			if (sig->type == TRX_SIG_COMMIT)
				return(FALSE);

			sig = UT_LIST_GET_NEXT(signals, sig);
		}

		return TRUE;
	}
	else if(type == TRX_SIG_BREAK_EXECUTION)
		return TRUE;
	else{
		ut_error;
		return FALSE;
	}

	return FALSE;
}

/*向一个事务对象发送一个signal,成功返回TRUE*/
ibool trx_sig_send(trx_t* trx, ulint type, ulint sender, ibool reply, que_thr_t* receiver_thr, trx_savept_t* savept, que_thr_t** next_thr)
{
	trx_sig_t*	sig;
	trx_t*		receiver_trx;

	ut_ad(trx);
	ut_ad(mutex_own(&kernel_mutex));

	/*信号冲突*/
	if(!trx_sig_is_compatible(trx, type, sender)){
		ut_a(0);
		return FALSE;
	}

	/*分配信号对象*/
	if(UT_LIST_GET_LEN(trx->signals) == 0)
		sig = &(trx->sig);
	else
		sig = mem_alloc(sizeof(trx_sig_t));

	UT_LIST_ADD_LAST(signals, trx->signals, sig);

	/*设置signal*/
	sig->type = type;
	sig->state = TRX_SIG_WAITING;
	sig->sender = sender;
	sig->reply = reply;
	sig->receiver = receiver_thr;

	if(savept)
		sig->savept = savept;

	if(receiver_thr){
		receiver_trx = thr_get_trx(receiver_thr);
		UT_LIST_ADD_LAST(reply_signals, receiver_trx->reply_signals, sig);
	}

	if(trx->sess->state == SESS_ERROR)
		trx_sig_reply_wait_to_suspended(trx);

	if(sender != TRX_SIG_SELF || type == TRX_SIG_BREAK_EXECUTION){
		ut_a(0);
		sess_raise_error_low(trx, 0, 0, NULL, NULL, NULL, NULL, "Signal from another session, or a break execution signal");
	}
	/*激活第一个信号线程对应的是新插入的信号，也就是说这个事务只有一个signal*/
	if(UT_LIST_GET_FIRST(trx->signals) == sig)
		trx_sig_start_handle(trx, next_thr);

	return TRUE;
}

void trx_end_signal_handling(trx_t* trx)
{
	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(trx->handling_signals);

	trx->handling_signals = FALSE;
	trx->graph = trx->graph_before_signal_handling;

	if(trx->graph && trx->sess->state == SESS_ERROR)
		que_fork_error_handler(trx, trx->graph);
}

/*激活一个事务的signal*/
void trx_sig_start_handle(trx_t* trx, que_thr_t* next_thr)
{
	trx_sig_t*	sig;
	ulint		type;

loop:
	ut_ad(trx);
	ut_ad(mutex_own(&kernel_mutex));

	/*有个信号正在被执行,但trx->signals中没有信号*/
	if(trx->handling_signals && UT_LIST_GET_LEN(trx->signals) == 0){
		trx_end_signal_handling(trx);
		return ;
	}

	/*激活事务*/
	if(trx->conc_state == TRX_NOT_STARTED)
		trx_start_low(trx, ULINT_UNDEFINED);

	if(trx->que_state == TRX_QUE_LOCK_WAIT)
		trx_lock_wait_to_suspended(trx);
	
	/*如果session错误，所有的线程都要等待reply signal信号*/
	if(trx->sess != NULL && trx->sess->state == SESS_ERROR)
		trx_sig_reply_wait_to_suspended(trx);

	if(trx->n_active_thrs > 0)
		return ;

	if(!trx->handling_signals){
		trx->graph_before_signal_handling = trx->graph;
		trx->handling_signals = TRUE;
	}

	sig = UT_LIST_GET_FIRST(trx->signals);
	type = sig->type;

	if(type == TRX_SIG_COMMIT){ /*提交信号，进行事务提交*/
		trx_handle_commit_sig_off_kernel(trx, next_thr);
	}
	else if(type == TRX_SIG_TOTAL_ROLLBACK || type == TRX_SIG_ROLLBACK_TO_SAVEPT){ /*指定执行回滚*/
		trx_rollback(trx, sig, next_thr);
		return ;
	}
	else if(type == TRX_SIG_ERROR_OCCURRED){ /*事务信号执行错误，回滚*/
		trx_rollback(trx, sig, next_thr);
		return 
	}
	else if(type == TRX_SIG_BREAK_EXECUTION){
		trx_sig_reply(trx, sig, next_thr);
		trx_sig_remove(trx, sig);
	}
	else{
		ut_error;
	}

	goto loop;
}

void trx_sig_reply(trx_t* trx, trx_sig_t* sig, que_thr_t** next_thr)
{
	trx_t*	receiver_trx;

	ut_ad(trx && sig);
	ut_ad(mutex_own(&kernel_mutex));

	if(sig->reply && sig->receiver != NULL){
		ut_ad((sig->receiver)->state == QUE_THR_SIG_REPLY_WAIT);
		receiver_trx = thr_get_trx(sig->receiver);

		UT_LIST_REMOVE(reply_signals, receiver_trx->reply_signals, sig);
		ut_ad(receiver_trx->sess->state != SESS_ERROR);

		que_thr_end_wait(sig->receiver, next_thr);

		sig->reply = FALSE;
		sig->receiver = NULL;
	}
	else if(sig->reply){
		sig->reply = FALSE;
		sig->receiver = NULL;

		sess_srv_msg_send_simple(trx->sess, SESS_SRV_SUCCESS, SESS_NOT_RELEASE_KERNEL);
	}
}

void trx_sig_remove(trx_t* trx, trx_sig_t* sig)
{
	ut_ad(trx && sig);
	ut_ad(mutex_own(&kernel_mutex));

	ut_ad(sig->reply == FALSE);
	ut_ad(sig->receiver == NULL);

	UT_LIST_REMOVE(signals, trx->signals, sig));

	if(sig != &(trx->sig))
		mem_free(sig);
}

commit_node_t* commit_node_create(mem_heap_t* heap)
{
	commit_node_t*	node;

	node = mem_heap_alloc(heap, sizeof(commit_node_t));
	node->common.type = QUE_NODE_COMMIT;
	node->state = COMMIT_NODE_SEND;

	return node;
}

/*在query graph中执行一个提交操作*/
que_thr_t* trx_commit_step(que_thr_t* thr)
{
	commit_node_t*	node;
	que_thr_t*	next_thr;
	ibool		success;

	node = thr->run_node;

	ut_ad(que_node_get_type(node) == QUE_NODE_COMMIT);

	if(thr->prev_node == que_node_get_parent(node))
		node->state = COMMIT_NODE_SEND;

	if(node->state == COMMIT_NODE_SEND){
		mutex_enter(&kernel_mutex);

		node->state = COMMIT_NODE_WAIT;
		next_thr = NULL;

		thr->state = QUE_THR_SIG_REPLY_WAIT;

		sucess = trx_sig_send(thr_get_trx(thr), TRX_SIG_COMMIT, TRX_SIG_SELF, TRUE, thr, NULL, &next_thr);

		mutex_exit(&kernel_mutex);

		if(!success)
			que_thr_handle_error(thr, DB_ERROR, NULL, 0);

		return next_thr;
	}

	ut_ad(node->state == COMMIT_NODE_WAIT);
	node->state = COMMIT_NODE_SEND;
	thr->run_node = que_node_get_parent(node);
}

ulint trx_commit_for_mysql(trx_t* trx)
{
	ut_a(trx);

	trx->op_info = "committing";
	trx_start_if_not_started(trx);

	mutex_enter(&kernel_mutex);
	trx_commit_off_kernel(trx);
	mutex_exit(&kernel_mutex);

	trx->op_info = "";
}

/*标记最新的SQL statement*/
void trx_mark_sql_stat_end(trx_t* trx)
{
	ut_a(trx);
	if (trx->conc_state == TRX_NOT_STARTED)
		trx->undo_no = ut_dulint_zero;

	trx->last_sql_stat_start.least_undo_no = trx->undo_no;
}

void trx_print(
	char*	buf,	/* in/out: buffer where to print, must be at least800 bytes */
	trx_t*	trx)	/* in: transaction */
{
        char*   start_of_line;

        buf += sprintf(buf, "TRANSACTION %lu %lu",
		ut_dulint_get_high(trx->id),
		 ut_dulint_get_low(trx->id));

  	switch (trx->conc_state) {
  		case TRX_NOT_STARTED:         buf += sprintf(buf,
						", not started"); break;
  		case TRX_ACTIVE:              buf += sprintf(buf,
						", ACTIVE %lu sec",
			 (ulint)difftime(time(NULL), trx->start_time)); break;
  		case TRX_COMMITTED_IN_MEMORY: buf += sprintf(buf,
						", COMMITTED IN MEMORY");
									break;
  		default: buf += sprintf(buf, " state %lu", trx->conc_state);
  	}

        buf += sprintf(buf, ", OS thread id %lu",
		       os_thread_pf(trx->mysql_thread_id));

	if (ut_strlen(trx->op_info) > 0) {
		buf += sprintf(buf, " %s", trx->op_info);
	}
	
  	if (trx->type != TRX_USER) {
    		buf += sprintf(buf, " purge trx");
  	}

	buf += sprintf(buf, "\n");
  	
	start_of_line = buf;

  	switch (trx->que_state) {
  		case TRX_QUE_RUNNING:         break;
  		case TRX_QUE_LOCK_WAIT:       buf += sprintf(buf,
						"LOCK WAIT "); break;
  		case TRX_QUE_ROLLING_BACK:    buf += sprintf(buf,
						"ROLLING BACK "); break;
  		case TRX_QUE_COMMITTING:      buf += sprintf(buf,
						"COMMITTING "); break;
  		default: buf += sprintf(buf, "que state %lu", trx->que_state);
  	}

  	if (0 < UT_LIST_GET_LEN(trx->trx_locks) ||
	    mem_heap_get_size(trx->lock_heap) > 400) {

  		buf += sprintf(buf,
"%lu lock struct(s), heap size %lu",
			       UT_LIST_GET_LEN(trx->trx_locks),
			       mem_heap_get_size(trx->lock_heap));
	}

  	if (trx->has_search_latch) {
  		buf += sprintf(buf, ", holds adaptive hash latch");
  	}

	if (ut_dulint_cmp(trx->undo_no, ut_dulint_zero) != 0) {
		buf += sprintf(buf, ", undo log entries %lu",
			ut_dulint_get_low(trx->undo_no));
	}
	
	if (buf != start_of_line) {

	        buf += sprintf(buf, "\n");
	}

  	if (trx->mysql_thd != NULL) {
    		innobase_mysql_print_thd(buf, trx->mysql_thd);
  	}  
}






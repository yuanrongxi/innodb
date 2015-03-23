#include "srv0que.h"

#include "srv0srv.h"
#include "os0thread.h"
#include "usr0sess.h"
#include "que0que.h"

/*检查并执行task queue中已有的que thread任务*/
void srv_que_task_queue_check()
{
	que_thr_t* thr;
	
	for(;;){
		mutex_enter(&kernel_mutex);
		thr = UT_LIST_GET_FIRST(srv_sys->tasks);

		if(thr == NULL){
			mutex_exit(&kernel_mutex);
			return ;
		}

		UT_LIST_REMOVE(queue, srv_sys->tasks, thr);
		mutex_exit(&kernel_mutex);

		que_run_threads(thr);		/*对thr的执行，有可能执行的时候会从新将thr的next thr插入到tasks queque的末尾*/
	}
}

/*将thr插入到tasks queue的末尾，取出queue中的第一个thr返回执行*/
que_thr_t* srv_que_round_robin(que_thr_t* thr)
{
	que_thr_t*	new_thr;

	ut_ad(thr);
	ut_ad(thr->state == QUE_THR_RUNNING);

	mutex_enter(&kernel_mutex);

	UT_LIST_ADD_LAST(queue, srv_sys->tasks, thr);
	new_thr = UT_LIST_GET_FIRST(srv_sys->tasks);

	mutex_exit(&kernel_mutex);

	return new_thr;
}

/*将一个que thread任务插入到tasks queue的末尾,已经对kernel mutex了*/
void srv_que_task_enqueue_low(que_thr_t* thr)
{
	ut_ad(thr);
	ut_ad(mutex_own(&kernel_mutex));

	UT_LIST_ADD_LAST(queue, srv_sys->tasks, thr);
	/*激活一个worker thread,让worker thread来执行que thread任务*/
	srv_release_threads(SRV_WORKER, 1);
}

/**/
void srv_que_task_enqueue(que_thr_t* thr)
{
	ut_ad(thr);

	mutex_enter(&kernel_mutex);
	srv_que_task_enqueue_low(thr);
	mutex_exit(&kernel_mutex);
}







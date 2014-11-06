#include "ut0wqueue.h"

UNIV_INTERN ib_queue_create()
{
	ib_wqueue_t* wq = static_cast<ib_wqueue_t*>(mem_alloc(sizeof(ib_wqueue_t)));

	/*创建mutex, SYNC_WORK_QUEUE是innodb内部定义的mutex类型*/
	mutex_create(PFS_NOT_INSTRUMENTED, &wq->mutex, SYNC_WORK_QUEUE);
	/*创建一个ib list*/
	wq->items = ib_list_create();
	/*创建一个系统信号量*/
	wq->event = os_event_create();
}

UNIV_INTERN void ib_wqueue_free(ib_wqueue_t* wq)
{
	mutex_free(wq->mutex);
	ib_list_free(wq->items);
	os_event_free(wq->event);

	mem_free(wq);
}

UNIV_INTERN void ib_wqueue_add(ib_wqueue_t wq, void* item, mem_heap_t* heap)
{
	mutex_enter(&wq->mutex);

	ib_list_add_last(wq->items, item, heap);
	/*发送信号给消息处理线程*/
	os_event_set(wq->event);

	mutex_exit(&wq->mutex);
}

UNIV_INTERN void* ib_wqueue_wait(ib_wqueue_t* wq)
{
	ib_list_node_t* node;

	for(;;){ /*进入读取queue状态，必须读到一个消息内容才退出循环处理*/
		os_event_wait(wq->event);

		mutex_enter(&wq->mutex);
		node = ib_list_get_first();
		if(node != NULL){ /*获取到了一个消息内容*/

			ib_list_remove(wq->items, node);
			if(!ib_list_get_first(wq->items)){
				os_event_reset(wq->event); /*queue里面没有消息了，必须重新设置等待信号*/
			}
			break;
		}
		mutex_exit(&wq->mutex);
	}

	mutex_exit(&wq->mutex);

	return node->data;
}

void* ib_wqueue_timedwait(ib_wqueue_t*	wq,	ib_time_t wait_in_usecs)	
{
	ib_list_node_t*	node = NULL;

	for (;;) {
		ulint		error;
		ib_int64_t	sig_count;

		mutex_enter(&wq->mutex);

		node = ib_list_get_first(wq->items);
		if (node){
			ib_list_remove(wq->items, node);

			mutex_exit(&wq->mutex);
			break;
		}
		/*没有取到消息的线程，重新设置等待信号*/
		sig_count = os_event_reset(wq->event);

		mutex_exit(&wq->mutex);
		/*设置等待的时间，并进行信号等待*/
		error = os_event_wait_time_low(wq->event, (ulint) wait_in_usecs, sig_count);

		if (error == OS_SYNC_TIME_EXCEEDED) /*假如等待超时*/
			break;
	}

	return(node ? node->data : NULL);
}

ibool ib_wqueue_is_empty(const ib_wqueue_t* wq)
{
	return ib_list_is_empty(wq->items);
}


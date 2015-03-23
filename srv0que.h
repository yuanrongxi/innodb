#ifndef __srv0que_h_
#define __srv0que_h_

/************************************************************************/
/* 任务队列管理                                                         */
/************************************************************************/

#include "univ.h"
#include "que0types.h"


void			srv_que_task_queue_check();

que_thr_t*		srv_que_round_robin(que_thr_t* thr);

void			srv_que_task_enqueue(que_thr_t* thr);

void			srv_que_task_enqueue_low(que_thr_t* thr);

#endif





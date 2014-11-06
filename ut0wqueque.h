#ifndef __IB_WORK_QUEUE_H
#define __IB_WORK_QUEUE_H

#include "ut0list.h"
#include "mem0mem.h"
#include "os0sync.h"
#include "sync0types.h"

struct ib_wqueue_t
{
	ib_mutex_t	mutex; /*互斥量*/
	ib_list_t*	items; /*用list作为queue的载体*/
	os_event_t	event; /*信号量*/
};

UNIV_INTERN ib_wqueue_t* ib_wqueue_create();

UNIV_INTERN void ib_wqueue_free(ib_wqueue_t* wq);

UNIV_INTERN void ib_wqueue_add(ib_wqueue_t* wq, void* item, mem_heap_t* heap);

ibool ib_wqueue_is_empty(const ib_wqueue_t* wq);

UNIV_INTERN void* ib_wqueue_wait(ib_wqueue_t* wq);

void* ib_wqueue_timewait(ib_wqueue_t* wq, ib_time_t wait_in_usecs);



#endif

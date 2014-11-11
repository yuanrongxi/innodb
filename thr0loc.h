#ifndef thr0loc_h
#define thr0loc_h

#include "univ.h"
#include "os0thread.h"

void		thr_local_init();

void		thr_local_create();

void		local_free(os_thread_id_t id);

ulint		thr_local_get_slot_no(os_thread_id_t id);

void		thr_local_set_slot_no(os_thread_id_t id, ulint slot_no);

ibool*		thr_local_get_in_ibuf_field();
#endif











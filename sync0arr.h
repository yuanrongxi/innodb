#ifndef sync0arr_h
#define sync0arr_h

#include "univ.h"
#include "ut0lst.h"
#include "ut0mem.h"
#include "os0thread.h"

typedef struct sync_cell_struct sync_cell_t;
typedef struct sync_array_struct sync_array_t;

#define SYNC_ARRAY_OS_MUTEX 1
#define SYNC_ARRAY_MUTEX	2

sync_array_t* sync_array_create(ulint n_cells, ulint protecton);

void sync_array_free(sync_array_t* arr);

void sync_array_reserve_cell(sync_array_t* arr, void* object, ulint type, char* file, ulint line, ulint* index);

void sync_array_wait_event(sync_array_t* arr, ulint index);

void sync_array_free_cell(sync_array_t* arr, ulint index);

void sync_array_signal_object(sync_array_t* arr, void* object);

void sync_arr_wake_threads_if_sema_free();

void sync_array_validate(sync_array_t* arr);

void sync_array_print_info(char* buf, char* buf_end, sync_array_t* arr);

#endif







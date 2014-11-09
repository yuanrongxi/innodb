#ifndef __HA0STORAGE_H_
#define __HA0STORAGE_H_

#include "univ.h"

#define HA_STORAGE_DEFAULT_HEAP_BYTES	1024
#define HA_STORAGE_DEFAULT_HASH_CELLS	4096

typedef struct ha_storage_struct ha_storage_t;

UNIV_INLINE ha_storage_t* ha_storage_create(ulint initial_heap_bytes, ulint initial_hash_cells);

UNIV_INLINE void ha_storage_empty(ha_storage_t** storage);

UNIV_INLINE void ha_storage_free(ha_storage_t* storage);

UNIV_INLINE ulint ha_storage_get_size(const ha_storage_t* storage);

UNIV_INTERN const void* ha_storage_put_memlim(ha_storage_t* storage, const void* data, ulint data_len, ulint memlim);

#define ha_storage_put(storage, data, data_len) ha_storage_put_memlim(storage, data, data_len, 0)

#define ha_storage_put_str(storage, str) ((const char*)ha_storage_put((storage), (str), strlen(str) + 1))

#define ha_storage_put_str_memlim(storage, str, memlim) \
	((const char*)ha_storage_put_memlim((storage), (str), strlen(str) + 1, (memlim)))



#endif

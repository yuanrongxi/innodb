#ifndef __rem0rec_h_
#define __rem0rec_h_

#include "univ.i"
#include "data0data.h"
#include "rem0types.h"
#include "mtr0types.h"


/*记录最大的列数*/
#define REC_MAX_N_FIELDS		(1024 - 1)
#define REC_MAX_HEAP_NO			(2 * 8192 - 1)
/*owned的最大值？*/
#define REC_MAX_N_OWNED			(16 - 1)

#define REC_INFO_MIN_REC_FLAG	0x10
/*记录扩展区的字节数量*/
#define REC_N_EXTRA_BYTES		6

#define REC_INFO_BITS			6

#define REC_1BYTE_OFFS_LIMIT	0x7f
#define REC_2BYTE_OFFS_LIMIT	0x7fff;

#define REC_MAX_DATA_SIZE		(16 * 1024)

UNIV_INLINE ulint			rec_get_next_offs(rec_t* rec);
UNIV_INLINE void			rec_set_next_offs(rec_t* rec, ulint next);

UNIV_INLINE ulint			rec_get_n_owned(rec_t* rec);
UNIV_INLINE void			rec_set_n_owned(rec_t* rect, ulint n_owned);

UNIV_INLINE ulint			rec_get_info_bits(rec_t* rec);
UNIV_INLINE void			rec_set_info_bits(rec_t* rec, ulint bits);

UNIV_INLINE ibool			rec_info_bits_get_deleted_flag(ulint info_bits);

UNIV_INLINE ibool			rec_get_deleted_flag(rec_t* rec);
UNIV_INLINE void			rec_set_deleted_flag(rec_t* rec, ibool flag);

UNIV_INLINE ulint			rec_get_heap_no(rec_t* rec);
UNIV_INLINE void			rec_set_heap_no(rec_t* rec, ulint heap_no);

UNIV_INLINE ibool			rec_get_1byte_offs_flag(rec_t* rec);
byte*						rec_get_nth_field(rec_t* rec, ulint n, ulint* len); 

UNIV_INLINE ulint			rec_get_nth_field_size(rec_t* rec, ulint n);

UNIV_INLINE ibool			rec_get_nth_field_extern_bit(rec_t* rec, ulint i);

UNIV_INLINE ibool			rec_contains_externally_stored_field(rec_t* rec);

void						rec_set_nth_field_extern_bit(rec_t* rec, ulint* vec, ulint n_fields, mtr_t* mtr);

UNIV_INLINE void			rec_copy_nth_field(void* buf, rec_t* rec, ulint n, ulint* len);
UNIV_INLINE void			rec_set_nth_field(rec_t* rec, ulint n, void* data, ulint len);

UNIV_INLINE ulint			rec_get_data_size(rec_t* rec);
UNIV_INLINE ulint			rec_get_extra_size(rec_t* rec);

UNIV_INLINE ulint			rec_get_size(rec_t* rec);
UNIV_INLINE byte*			rec_get_start(rec_t* rec);
UNIV_INLINE byte*			rec_get_end(rec_t* rec);
UNIV_INLINE rec_t*			rec_copy(void* buf, rec_t* rec);

rec_t*						rec_copy_prefix_to_buf(rec_t* rec, ulint n_fields, byte** buf, ulint* buf_size);

UNIV_INLINE ulint			rec_fold(rec_t* rec, ulint n_fields, ulint n_bytes, dulint tree_id);

UNIV_INLINE rec_t*			rec_convert_dtuple_to_rec(byte* destination, dtuple_t* dtuple);
rec_t*						rec_convert_dtuple_to_rec_low(byte* destination, dtuple_t* tuple, ulint data_size);

UNIV_INLINE ulint			rec_get_converted_extra_size(ulint data_size, ulint n_fields);
UNIV_INLINE ulint			rec_get_converted_size(dtuple_t* dtuple);

void						rec_copy_prefix_to_dtuple(dtuple_t* tuple, rec_t* rec, ulint n_fields, mem_heap_t* heap);

void						rec_validate(rec_t* rec);
void						rec_print(rec_t* rec);
ulint						rec_sprintf(char* buf, ulint buf_len, rec_t* rec);

#include "remrec.inl"

#endif






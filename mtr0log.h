#ifndef __MTR0LOG_H_
#define __MTR0LOG_H_

#include "univ.h"
#include "mtr0mtr.h"

void				mlog_write_ulint(byte* ptr, ulint val, byte type, mtr_t* mtr);

void				mlog_write_dulint(byte* ptr, dulint val, byte type, mtr_t* mtr);

void				mlog_write_string(byte* ptr, byte* str, ulint len, mtr_t* mtr);

void				mlog_write_initial_log_record(byte* ptr, byte type, mtr_t* mtr);

void				mlog_catenate_string(mtr_t* mtr, byte* str, ulint len);

UNIV_INLINE void	mlog_catenate_ulint(mtr_t* mtr, ulint val, ulint type);

UNIV_INLINE void	mlog_catenate_ulint_compressed(mtr_t* mtr, ulint val);

UNIV_INLINE	void	mlog_catenate_dulint_compressed(mtr_t* mtr, dulint val);

UNIV_INLINE byte*	mlog_open(mtr_t* mtr, ulint size);

UNIV_INLINE void	mlog_close(mtr_t* mtr, byte* ptr);

UNIV_INLINE byte*	mlog_write_initial_log_record_fast(byte* ptr, byte type, byte* log_ptr, mtr_t* mtr);

UNIV_INLINE dulint	mlog_write(dyn_array_t* mlog, ibool* modifications);

byte*				mlog_parse_initial_log_record(byte* ptr, byte* end_ptr, byte* type, ulint* space, ulint* page_no);

byte*				mlog_parse_nbytes(ulint type, byte* ptr, byte* end_ptr, byte* page);

byte*				mlog_parse_string(byte* ptr, byte* end_ptr, byte* page);

#define MLOG_BUF_MARGIN 256

#include "mtr0log.inl"
#endif






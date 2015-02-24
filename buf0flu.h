#ifndef __buf0flu_h_
#define __buf0flu_h_

#include "unvi.h"
#include "buf0types.h"
#include "ut0byte.h"
#include "mtr0types.h"

#define BUF_FLUSH_FREE_BLOCK_MARGIN 	(5 + BUF_READ_AHEAD_AREA)
#define BUF_FLUSH_EXTRA_MARGIN 			(BUF_FLUSH_FREE_BLOCK_MARGIN / 4 + 100)



void									buf_flush_write_complete(buf_block_t* block);

void									buf_flush_free_margin();

void									buf_flush_init_for_writing(byte* page, dulint newest_lsn, ulint space, ulint page_no);

ulint									buf_flush_batch(ulint flush_type, ulint min_n, dulint lsn_limit);

void									buf_flush_wait_batch_end(ulint type);

UNIV_INLINE void						buf_flush_note_modification(buf_block_t* block, mtr_t* mtr);

UNIV_INLINE void						buf_flush_recv_note_modification(buf_block_t* block, dulint start_lsn, dulint end_lsn);

ibool									buf_flush_ready_for_replace(buf_block_t* block);

ibool									buf_flush_validate();

#include "buf0flu.h"

#endif





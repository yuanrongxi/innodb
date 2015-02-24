/*********************************************************************
*从磁盘中将page数据读入buffer pool中，这里包括正常的页读取和预读页数据
*预读包括：
*		指定预读
*		随机预读
*********************************************************************/
#ifndef __buf0rea_h_
#define __buf0rea_h_

#include "univ.h"
#include "buf0types.h"
#include "buf0buf.h"

#define	BUF_READ_AHEAD_AREA				ut_min(64, ut_2_power_up(buf_pool->curr_size / 32))

/* Modes used in read-ahead */
#define BUF_READ_IBUF_PAGES_ONLY		131
#define BUF_READ_ANY_PAGE				132

ulint									buf_read_page(ulint space, ulint offset);
ulint									buf_read_ahead_linear(ulint space, ulint offset);
void									buf_read_ibuf_merge_pages(ibool sync, ulint space, ulint* page_nos, ulint n_stored);
void									buf_read_recv_pages(iool sync, ulint space, ulint* page_nos, ulint n_stored);

#endif

/*******************************************************************/





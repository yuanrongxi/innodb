#ifndef __buf0buf_hi_
#define __buf0buf_hi_

#include "univ.h"
#include "fil0fil.h"
#include "buf0types.h"
#include "sync0rw.h"
#include "hash0hash.h"
#include "ut0byte.h"
#include "mtr0types.h"

/*Flags for flush types*/
#define	BUF_FLUSH_LRU			1
#define BUF_FLUSH_SINGLE_PAGE	2
#define BUF_FLUSH_LIST			3

#define BUF_GET					10
#define BUF_GET_IF_IN_POOL		11
#define BUF_GET_NOWAIT			12
#define BUF_GET_NO_LATCH		14
#define BUF_MAKE_YOUNG			51
#define BUF_KEEP_OLD			52

/*control block state定义*/
#define BUF_BLOCK_NOT_USED		211
#define BUF_BLOCK_READY_FOR_USE	212
#define BUF_BLOCK_FILE_PAGE		213
#define BUF_BLOCK_MEMORY		214
#define BUF_BLOCK_REMOVE_HASH	215

/* Io_fix states of a control block; these must be != 0 */
#define BUF_IO_READ				561
#define BUF_IO_WRITE			562

/*block magic value*/
#define BUF_BLOCK_MAGIC_N	41526563

typedef struct buf_block_struct buf_block_t;
/*buf_block_t的定义*/
struct buf_block_struct
{
	ulint						magic_n;			/*魔法字校验*/
	ulint						state;				/*block状态*/
	byte*						frame;				/*一块大小UNIV_PAGE_SIZE对齐的内存*/
	ulint						space;				/*space id*/
	ulint						offset;				/*page number*/
	ulint						lock_hash_val;		
	mutex_t*					lock_mutex;
	rw_lock_t					lock;
	rw_lock_t					read_lock;
	buf_block_t*				hash;

	/*flush相关变量*/
	UT_LIST_NODE_T(buf_block_t) flush_list; /*被修改过的block列表*/
	dulint						newest_modification;
	dulint						oldest_modification;
	ulint						flush_type;

	/*LRU*/
	UT_LIST_NODE_T(buf_block_t) free;
	UT_LIST_NODE_T(buf_block_t) LRU;
	ulint						LRU_position;
	ulint						freed_page_clock;
	ibool						old;
	ibool						accessed;		/*block是否被buffer pool缓冲过，如果没有accessed = FALSE*/
	ulint						buf_fix_count;  /*对应的page正在被外部调用的对象的计数器*/
	ulint						io_fix;			/*是否有IO操作正在对block对应的page做操作*/

	dulint						modify_clock;

	/*自适应hash索引相关*/
	ulint						n_hash_helps;
	ulint						n_fields;
	ulint						n_bytes;
	ulint						side;
	ibool						is_hashed;
	ulint						curr_n_fields;
	ulint						curr_n_bytes;
	ulint						curr_side;

	/*调试相关*/
	rw_lock_t					debug_latch;
	ibool						file_page_was_freed;
};

/*buf_pool_t定义*/
typedef struct buf_pool_t
{
	mutex_t						mutex;
	byte*						frame_mem;
	byte*						frame_zero;		/*第一个block frame的指针地址*/
	byte*						high_end;
	buf_block_t*				blocks;			/*块数组*/

	ulint						max_size;
	ulint						curr_size;
	hash_table_t*				page_hash;
	ulint						n_pend_reads;
	time_t						last_printout_time;

	ulint						n_pages_read;
	ulint						n_pages_written;
	ulint						n_pages_created;
	ulint						n_page_gets;
	ulint						n_page_gets_old;
	ulint						n_pages_read_old;
	ulint						n_pages_written_old;
	ulint						n_pages_created_old;

	/*Page flush相关*/
	UT_LIST_BASE_NODE_T(buf_block_t) flush_list;
	ibool						init_flush[BUF_FLUSH_LIST + 1];
	ulint						n_flush[BUF_FLUSH_LIST + 1];
	os_event_t					no_flush[BUF_FLUSH_LIST + 1];
	ulint						ulint_clock;
	ulint						freed_page_clock;
	ulint						LRU_flush_ended;

	/*LRU相关*/
	UT_LIST_BASE_NODE_T(buf_block_t) free;
	UT_LIST_BASE_NODE_T(buf_block_t) LRU;
	buf_block_t*				LRU_old;
	ulint						LRU_old_len;
}buf_pool_t;

/************************宏封装函数****************************************************
NOTE! The following macros should be used instead of buf_page_get_gen,
to improve debugging. Only values RW_S_LATCH and RW_X_LATCH are allowed
in LA! */
#define buf_page_get(SP, OF, LA, MTR)    buf_page_get_gen(\
	SP, OF, LA, NULL,\
	BUF_GET, __FILE__, __LINE__, MTR)

/******************************************************************
Use these macros to bufferfix a page with no latching. Remember not to
read the contents of the page unless you know it is safe. Do not modify
the contents of the page! We have separated this case, because it is
error-prone programming not to set a latch, and it should be used
with care. */
#define buf_page_get_with_no_latch(SP, OF, MTR)    buf_page_get_gen(\
	SP, OF, RW_NO_LATCH, NULL,\
	BUF_GET_NO_LATCH, __FILE__, __LINE__, MTR)
/******************************************************************
NOTE! The following macros should be used instead of buf_page_get_gen, to
improve debugging. Only values RW_S_LATCH and RW_X_LATCH are allowed as LA! */
#define buf_page_get_nowait(SP, OF, LA, MTR)    buf_page_get_gen(\
	SP, OF, LA, NULL,\
	BUF_GET_NOWAIT, __FILE__, __LINE__, MTR)
/******************************************************************
NOTE! The following macros should be used instead of
buf_page_optimistic_get_func, to improve debugging. Only values RW_S_LATCH and
RW_X_LATCH are allowed as LA! */
#define buf_page_optimistic_get(LA, G, MC, MTR) buf_page_optimistic_get_func(\
	LA, G, MC, __FILE__, __LINE__, MTR)

/************************函数申明******************************************************/
void							buf_pool_init(ulint max_size, ulint curr_size);

UNIV_INLINE ulint				buf_pool_get_curr_size();
UNIV_INLINE ulint				buf_pool_get_max_size();
UNIV_INLINE dulint				buf_pool_get_oldest_modification();

buf_frame_t*					buf_frame_alloc();
void							buf_frame_free(buf_frame_t* frame);
UNIV_INLINE byte*				buf_frame_copy(byte* buf, buf_frame_t* frame);

ibool							buf_page_optimistic_get_func(ulint rw_latch, buf_frame_t* guess, dulint modify_clock, char* file, ulint line, mtr_t* mtr);
UNIV_INLINE	buf_frame_t*		buf_page_get_release_on_io(ulint space, ulint offset, buf_frame_t* guess, ulint rw_latch, ulint savepoint, mtr_t* mtr);
ibool							buf_page_get_known_nowait(ulint rw_latch, buf_frame_t* guess, ulint mode, char* file, ulint line, mtr_t* mtr);
buf_frame_t*					buf_page_get_gen(ulint space, ulint offset, ulint rw_latch, buf_frame_t* guess, ulint mode, char* file, ulint line, mtr_t* mtr);

buf_frame_t*					buf_page_create(ulint space, ulint offset, mtr_t* mtr);
UNIV_INLINE void				buf_page_release(buf_block_t* block, ulint rw_latch, mtr_t* mtr);
void							buf_page_make_young(buf_frame_t* frame);
ibool							buf_page_peek(ulint space, ulint offset);
buf_block_t*					buf_page_peek_block(ulint space, ulint offset);
buf_block_t*					buf_page_set_file_page_was_freed(ulint space, ulint offset);
buf_block_t*					buf_page_reset_file_page_was_freed(ulint space, ulint offset);

UNIV_INLINE ibool				buf_block_peek_if_too_old(buf_block_t* block);
ibool							buf_page_peek_if_search_hashed(ulint space, ulint offset);

UNIV_INLINE dulint				buf_frame_get_newest_modification(buf_frame_t* frame);
UNIV_INLINE dulint				buf_frame_modify_clock_inc(buf_frame_t* frame);
UNIV_INLINE dulint				buf_frame_get_modify_clock(buf_frame_t* frame);

ulint							buf_calc_page_checksum(byte* page);
ibool							buf_page_is_corrupted(byte* read_buf);
UNIV_INLINE ulint				buf_frame_get_page_no(byte* ptr);
UNIV_INLINE ulint				buf_frame_get_space_id(byte* ptr);
UNIV_INLINE void				buf_ptr_get_fsp_addr(byte* ptr, ulint* space, fil_addr_t* addr);
UNIV_INLINE ulint				buf_frame_get_lock_hash_val(byte* ptr);
UNIV_INLINE mutex_t*			buf_frame_get_lock_mutex(byte* ptr);
UNIV_INLINE buf_frame_t*		buf_frame_align(byte* ptr);
UNIV_INLINE	ibool				buf_pool_is_block(void* ptr);

ibool							buf_validate();
void							buf_page_print(byte* read_buf);
void							buf_print();

ulint							buf_get_n_pending_ios();
void							buf_print_io(char* buf, char* buf_end);
void							buf_refresh_io_stats();
ibool							buf_all_freed();
ibool							buf_pool_check_no_pending_io();
void							buf_pool_invalidate();

UNIV_INLINE	void				buf_page_dbg_add_level(buf_frame_t* frame, ulint level);
UNIV_INLINE buf_frame_t*		buf_block_get_frame(buf_block_t* block);
UNIV_INLINE ulint				buf_block_get_space(buf_block_t* block);
UNIV_INLINE ulint				buf_block_get_page_no(buf_block_t* block);
UNIV_INLINE buf_block_t*		buf_block_align(byte* ptr);
UNIV_INLINE ibool				buf_page_io_query(buf_block_t* block);
UNIV_INLINE buf_block_t*		buf_pool_get_nth_block(buf_pool_t* pool, ulint i);

buf_block_t*					buf_page_init_for_read(ulint mode, ulint space, ulint offset);
void							buf_page_io_complete(buf_block_t* block);
UNIV_INLINE ulint				buf_page_address_fold(ulint space, ulint offset);
UNIV_INLINE buf_block_t*		buf_page_hash_get(ulint space, ulint offset);

UNIV_INLINE ulint				buf_pool_clock_tic();
ulint							buf_get_free_list_len();

/*全局缓冲池对象*/
extern buf_pool_t*				buf_pool;
extern ibool					buf_debug_prints;

#include "buf0buf.inl"

#endif






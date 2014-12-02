#ifndef __MTR0MTR_H_
#define __MTR0MTR_H_

#include "univ.h"
#include "mem0mem.h"
#include "dyn0dyn.h"
#include "buf0types.h"
#include "sync0rw.h"
#include "ut0byte.h"
#include "mtr0types.h"
#include "page0types.h"


#define MTR_LOG_ALL				21	/*所有的log操作是基于磁盘的数据修改*/
#define MTR_LOG_NONE			22	/*log无操作*/
#define MTR_LOG_SHORT_INSERTS	24	/*log操作仅仅修改space和page中的数据分配*/

#define	MTR_MEMO_PAGE_S_FIX		RW_S_LATCH
#define	MTR_MEMO_PAGE_X_FIX		RW_X_LATCH
#define	MTR_MEMO_BUF_FIX		RW_NO_LATCH
#define MTR_MEMO_MODIFY			54
#define	MTR_MEMO_S_LOCK			55
#define	MTR_MEMO_X_LOCK			56

#define	MLOG_SINGLE_REC_FLAG	128

#define MLOG_1BYTE				((byte)1)
#define MLOG_2BYTES				((byte)2)
#define MLOG_4BYTES				((byte)4)
#define MLOG_8BYTES				((byte)8)

/*mtr操作的类型*/
#define	MLOG_REC_INSERT				((byte)9)
#define	MLOG_REC_CLUST_DELETE_MARK	((byte)10) 
#define	MLOG_REC_SEC_DELETE_MARK	((byte)11)
#define MLOG_REC_UPDATE_IN_PLACE	((byte)13)
#define MLOG_REC_DELETE				((byte)14)
#define	MLOG_LIST_END_DELETE 		((byte)15)
#define	MLOG_LIST_START_DELETE 		((byte)16)
#define	MLOG_LIST_END_COPY_CREATED  ((byte)17)
#define	MLOG_PAGE_REORGANIZE 		((byte)18)
#define MLOG_PAGE_CREATE 			((byte)19)
#define	MLOG_UNDO_INSERT 			((byte)20)
#define MLOG_UNDO_ERASE_END			((byte)21)
#define	MLOG_UNDO_INIT 				((byte)22)
#define MLOG_UNDO_HDR_DISCARD		((byte)23)
#define	MLOG_UNDO_HDR_REUSE			((byte)24)
#define MLOG_UNDO_HDR_CREATE		((byte)25)
#define MLOG_REC_MIN_MARK			((byte)26)	
#define MLOG_IBUF_BITMAP_INIT		((byte)27)
#define	MLOG_FULL_PAGE				((byte)28)
#define MLOG_INIT_FILE_PAGE			((byte)29)
#define MLOG_WRITE_STRING			((byte)30)
#define	MLOG_MULTI_REC_END			((byte)31)
#define MLOG_DUMMY_RECORD			((byte)32)
#define MLOG_BIGGEST_TYPE			((byte)32)

typedef struct mtr_memo_slot_struct
{
	ulint			type;
	void*			object;
}mtr_memo_slot_t;

typedef struct mtr_struct
{
	ulint			space;
	dyn_array_t		memo;
	dyn_array_t		log;
	ibool			modifications;
	ulint			n_log_recs;
	ulint			log_mode;			/*log操作模式，MTR_LOG_ALL、MTR_LOG_NONE、MTR_LOG_SHORT_INSERTS*/
	dulint			start_lsn;			
	dulint			end_lsn;
	ulint			magic_n;			/*魔法字*/
}mtr_t;

UNIV_INLINE mtr_t*		mtr_start(mtr_t* mtr);

UNIV_INLINE ulint		mtr_set_savepoint(mtr_t* mtr, ulint savepoint);

UNIV_INLINE void		mtr_release_s_latch_at_savepoint(mtr_t* mtr, ulint savepoint, rw_lock_t* lock);

UNIV_INLINE ulint		mtr_get_log_mode(mtr_t* mtr);

UNIV_INLINE ulint		mtr_set_log_mode(mtr_t* mtr, ulint mode);

UNIV_INLINE void		mtr_s_lock_func(rw_lock_t* lock, char* file, ulint line, mtr_t* mtr);

UNIV_INLINE void		mtr_x_lock_func(rw_lock_t* lock, char* file, ulint line, mtr_t* mtr);

UNIV_INLINE ibool		mtr_memo_contains(mtr_t* mtr, void* object, ulint type);

mtr_t*					mtr_start_noninline(mtr_t* mtr);

void					mtr_commit(mtr_t* mtr);

void					mtr_log_write_backup_entries(mtr_t* mtr, dulint backup_lsn);			

void					mtr_rollback_to_savepoint(mtr_t* mtr, ulint savepoint);

ulint					mtr_read_ulint(byte* ptr, ulint type, mtr_t* mtr);

dulint					mutr_read_dulint(byte* ptr, ulint type, mtr_t* mtr);

void					mtr_memo_release(mtr_t* mtr, void* object, ulint type);

byte*					mtr_log_parse_full_page(byte* ptr, byte* end_ptr, page_t* page);

void					mtr_print(mtr_t* mtr);

UNIV_INLINE dyn_array_t* mtr_get_log(mtr_t* mtr);

UNIV_INLINE void		mtr_memo_push(mtr_t* mtr, void* object, ulint type);

#define mtr_s_lock(B, MTR)	mtr_s_lock_func((B), IB__FILE__, __LINE__, (MTR))
#define mtr_x_lock(B, MTR)  mtr_x_lock_func((B), IB__FILE__, __LINE__, (MTR))

#define	MTR_BUF_MEMO_SIZE	200

#define	MTR_MAGIC_N			54551

#define MTR_ACTIVE			12231
#define MTR_COMMITTING		56456
#define MTR_COMMITTED		34676

#include "mtr0mtr.inl"

#endif




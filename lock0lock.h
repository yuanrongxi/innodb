#ifndef __LOCK0LOCK_H_
#define __LOCK0LOCK_H_

#include "univ.h"
#include "trx0types.h"
#include "rem0types.h"
#include "dict0types.h"
#include "que0types.h"
#include "page0types.h"
#include "lock0types.h"
#include "read0types.h"
#include "hash0hash.h"

extern ibool			lock_print_waits;

/*获得锁的结构体长度*/
ulint					lock_get_size();

/*建立数据库的锁系统*/
void					lock_sys_create(ulint n_cells);

trx_t*					lock_sec_rec_some_has_impl_off_kernel(rec_t* rec, dict_index_t* index);

UNIV_INLINE trx_t*		lock_clust_rec_some_has_impl(rec_t* rec, dict_index_t* index);

void					lock_rec_reset_and_release_wait(rec_t* rec);

void					lock_rec_inherit_to_gap(rec_t* heir, rec_t* rec);

void					lock_move_reorganize_page(page_t* page, page_t* new_page);

void					lock_move_rec_list_end(page_t* new_page, page_t* page, rec_t* rec);

void					lock_move_rec_list_start(page_t* new_page, page_t* page, rec_t* rec, rec_t* old_end);

void					lock_update_split_right(page_t* right_page, page_t* left_page);

void					lock_update_merge_right(rec_t* orig_succ, page_t* left_page);

void					lock_update_root_raise(page_t* new_page, page_t* root);

void					lock_update_copy_and_discard(page_t* new_page, page_t* page);

void					lock_update_split_left(page_t* right_page, rec_t* left_page);

void					lock_update_merge_left(page_t* left_page, rec_t* orig_pred, page_t* right_page);

void					lock_rec_reset_and_inherit_gap_locks(rec_t* heir, rec_t* rec);

void					lock_update_discard(rec_t* heir, page_t* page);

void					lock_update_insert(rec_t* rec);

void					lock_update_delete(rec_t* rec);

void					lock_rec_store_on_page_infimum(rec_t* rec);

void					lock_rec_restore_from_page_infimum(rec_t* rec, page_t* page);

ibool					lock_rec_expl_exist_on_page(ulint space, ulint page_no);

ulint					lock_rec_insert_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, que_thr_t* thr, ibool inherit);

ulint					lock_clust_rec_modify_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, que_thr_t* thr);

ulint					lock_sec_rec_modify_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, que_thr_t* thr);

ulint					lock_clust_rec_read_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, ulint mode, que_thr_t* thr);

ulint					lock_sec_rec_read_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, ulint mode, que_thr_t* thr);

ibool					lock_clust_rec_cons_read_sees(rec_t* rec, dict_index_t* index, read_view_t* view);

ulint					lock_sec_rec_cons_read_sees(rec_t* rec, dict_index_t* index, read_view_t* view);

ulint					lock_table(ulint flags, dict_table_t* table, ulint mode, que_thr_t* thr);

ibool					lock_is_on_table(dict_table_t* table);

void					lock_table_unlock_auto_inc(trx_t* trx);

void					lock_release_off_kernel(trx_t* trx);

void					lock_cancel_waiting_and_release(lock_t* lock);

void					lock_reset_all_on_table(dict_table_t* table);

UNIV_INLINE ulint		lock_rec_fold(ulint space, ulint page_no);

UNIV_INLINE ulint		lock_rec_hash(ulint space, ulint page_no);

mutex_t*				lock_rec_get_mutex_for_addr(ulint space, ulint page_no);

ibool					lock_rec_queue_validate(rec_t* rec, dict_index_t* index);

void					lock_table_print(char* buf, lock_t* lock);

void					lock_rec_print(char* buf, lock_t* lock);

void					lock_print_info(char* buf, char* buf_end);

ibool					lock_table_queue_validate(dict_table_t* table);

ibool					lock_rec_validate_page(ulint space, ulint page_no);

ibool					lock_validate();

#define LOCK_NONE		0
/*意向共享锁*/
#define LOCK_IS			2
/*意向独占锁*/
#define LOCK_IX			3	
/*共享锁*/
#define LOCK_S			4
/*独占锁*/
#define LOCK_X			5
/*自增长锁*/
#define LOCK_AUTO_INC	6

#define LOCK_MODE_MASK	0xF

#define LOCK_TABLE		16

#define LOCK_REC		32

#define LOCK_TYPE_MASK	0xF0

#define LOCK_WAIT		256

#define LOCK_GAP		512

#define LOCK_RELEASE_WAIT 1
#define LOCK_NOT_RELEASE_WAIT 2

typedef struct lock_op_struct
{
	dict_table_t*		table;
	ulint				mode;
}lock_op_t;

#define LOCK_OP_START		1
#define LOCK_OP_COMPLETE	2

struct lock_sys_struct
{
	hash_table_t*	rec_hash;
};

extern lock_sys_t*		lock_sys;

#include "lock0lock.inl"

#endif





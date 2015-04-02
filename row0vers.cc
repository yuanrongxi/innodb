#include "row0vers.h"
#include "dict0dict.h"
#include "dict0boot.h"
#include "btr0btr.h"
#include "mach0data.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0roll.h"
#include "trx0undo.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "que0que.h"
#include "row0row.h"
#include "row0upd.h"
#include "rem0cmp.h"
#include "read0read.h"
#include "page0page.h"
#include "log0recv.h"
#include "read0read.h"


/*******************************************************************************/
/*从聚集索引对应的记录中读取trx id*/
UNIV_INLINE dulint row_vers_get_trx_id(rec_t* rec, dict_table_t* table)
{
	return row_get_rec_trx_id(rec, dict_table_get_first_index(table));
}

/*在index索引上是否可以执行一致性读操作*/
UNIV_INLINE ibool row_vers_clust_rec_sees_older(rec_t* rec, dict_index_t* index, read_view_t* view)
{
	ut_ad(index->type & DICT_CLUSTERED);

	if(read_view_sees_trx_id(view, row_get_rec_trx_id(rec, index)))
		return FALSE;

	return TRUE;
}

/*检查二级索引是否可以立即执行一致性读或者读取旧版本记录，如果可以，返回TRUE，并直接从聚集索引上返回记录*/
UNIV_INLINE ibool row_vers_sec_rec_may_see_older(rec_t* rec, dict_index_t* index, read_view_t* view)
{
	page_t*	page;

	ut_ad(!(index->type & DICT_CLUSTERED));

	page = buf_frame_align(rec);
	if ((ut_dulint_cmp(page_get_max_trx_id(page), view->up_limit_id) >= 0) || recv_recovery_is_on())  /*page上的max trx id比view上限大，说明读事务在修改page事务的后面*/
		return TRUE;

	return FALSE;
}

/*******************************************************************************/
/*找出插入和修改记录的二级索引的事务,事务必须在active状态*/
trx_t* row_vers_impl_x_locked_off_kernel(rec_t* rec, dict_index_t* index)
{
	dict_index_t*	clust_index;
	rec_t*		clust_rec;
	rec_t*		version;
	rec_t*		prev_version;
	dulint		trx_id;
	dulint		prev_trx_id;
	mem_heap_t*	heap;
	mem_heap_t*	heap2;
	dtuple_t*	row;
	dtuple_t*	entry	= NULL; /* assignment to eliminate compiler
					warning */
	trx_t*		trx;
	ibool		vers_del;
	ibool		rec_del;
	ulint		err;
	mtr_t		mtr;
	char        err_buf[1000];

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(!rw_lock_own(&(purge_sys->latch), RW_LOCK_SHARED));

	mutex_exit(&kernel_mutex);

	mtr_start(&mtr);
	/*获得聚集索引上对应的记录*/
	clust_rec = row_get_clust_rec(BTR_SEARCH_LEAF, rec, index, &clust_index, &mtr);
	if(clust_rec == NULL){ /*聚集索引上的记录不存在*/
		mutex_enter(&kernel_mutex);
		mtr_commit(&mtr);

		return NULL;
	}

	/*获得聚集索引上的事务ID*/
	trx_id = row_get_rec_trx_id(clust_rec, clust_index);
	mtr_s_lock(&purge_sys->latch, &mtr);

	mutex_enter(&kernel_mutex);
	if(!trx_is_active(trx_id)){ /*事务必须是active状态，也就是持有记录的行锁*/
		mtr_commit(&mtr);
		return NULL;
	}

	rec_del = rec_get_deleted_flag(rec);
	trx = NULL;

	version = clust_rec;
	heap = NULL;

	for(;;){
		mutex_exit(&kernel_mutex);

		/* While we retrieve an earlier version of clust_rec, we
		release the kernel mutex, because it may take time to access
		the disk. After the release, we have to check if the trx_id
		transaction is still active. We keep the semaphore in mtr on
		the clust_rec page, so that no other transaction can update
		it and get an implicit x-lock on rec. */

		heap2 = heap;
		heap = mem_heap_create(1024);
		/*获得一个更早的记录版本*/
		err = trx_undo_prev_version_build(clust_rec, &mtr, version, clust_index, heap, &prev_version);
		if(heap2 != NULL)
			mem_heap_free(heap2);

		if(prev_version != NULL){
			row = row_build(ROW_COPY_POINTERS, clust_index, prev_version, heap);
			entry = row_build_index_entry(row, index, heap);
		}

		mutex_enter(&kernel_mutex);
		if(!trx_is_active(trx_id))
			break;

		ut_ad(err == DB_SUCCESS);
		if(prev_version == NULL){
			trx = trx_get_on_id(trx_id);
			break;
		}

		vers_del = rec_get_deleted_flag(prev_version);
		if(0 == cmp_dtuple_rec(entry, rec)){
			if (rec_del != vers_del) { /*如果记录内容相同，但删除标识不一样，说明记录前面是被修改过的，其他事务不能同时修改这个记录，有隐式锁*/
				trx = trx_get_on_id(trx_id);
				break;
			}
		}
		else if(!rec_del){ /*说明主索引记录在原来记录的物理位置进行了修改*/
			trx = trx_get_on_id(trx_id);
			break;
		}

		prev_trx_id = row_get_rec_trx_id(prev_version, clust_index);
		if (0 != ut_dulint_cmp(trx_id, prev_trx_id)) {
			/* The versions modified by the trx_id transaction end to prev_version: no implicit x-lock */
			break;
		}

		version = prev_version;
	}

	mtr_commit(&mtr);
	mem_heap_free(heap);
}

/*假如trx id >= purge view trx ids, 需要对一个处于del mark的记录早期版本做删除保护,如果需要保护返回TRUE*/
ibool row_vers_must_preserve_del_marked(dulint trx_id, mtr_t* mtr)
{
	ut_ad(!rw_lock_own(&(purge_sys->latch), RW_LOCK_SHARED));

	mtr_s_lock(&(purge_sys->latch), mtr);

	if (trx_purge_update_undo_must_exist(trx_id)) {
		/* A purge operation is not yet allowed to remove this delete marked record */
		return TRUE;
	}

	return FALSE;
}

/*********************************************************************
Finds out if a version of the record, where the version >= the current
purge view, should have ientry as its secondary index entry. We check
if there is any not delete marked version of the record where the trx
id >= purge view, and the secondary index entry == ientry; exactly in
this case we return TRUE. */
ibool row_vers_old_has_index_entry(ibool also_curr, rec_t* rec, mtr_t* mtr, dict_index_t* index, dtuple_t* ientry)
{
	rec_t*		version;
	rec_t*		prev_version;
	dict_index_t*	clust_index;
	mem_heap_t*	heap;
	mem_heap_t*	heap2;
	dtuple_t*	row;
	dtuple_t*	entry;
	ulint		err;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(rec), MTR_MEMO_PAGE_X_FIX) 
		|| mtr_memo_contains(mtr, buf_block_align(rec), MTR_MEMO_PAGE_S_FIX));
	ut_ad(!rw_lock_own(&(purge_sys->latch), RW_LOCK_SHARED));

	mtr_s_lock(&(purge_sys->latch), mtr);

	/*获得表的聚集索引对象*/
	clust_index = dict_table_get_first_index(index->table);
	if(also_curr && !rec_get_deleted_flag(rec)){
		heap = mem_heap_create(1024);
		row = row_build(ROW_COPY_POINTERS, clust_index, rec, heap);
		entry = row_build_index_entry(row, index, heap);

		/*聚集索引上的行与ientry相同*/
		if(dtuple_datas_are_ordering_equal(ientry, entry)){
			mem_heap_free(heap);
			return TRUE;
		}

		mem_heap_free(heap);
	}

	version = rec;
	heap = NULL;
	
	/*在更早的版本上找*/
	for(;;){
		heap2 = heap;
		heap = mem_heap_create(1024);

		err = trx_undo_prev_version_build(rec, mtr, version, clust_index, heap, &prev_version);	
		if(heap2)
			mem_heap_free(heap2);

		if(err != DB_SUCCESS && prev_version == NULL){ /*没有更早的版本*/
			mem_heap_free(heap);
			return FALSE;
		}

		if (!rec_get_deleted_flag(prev_version)){
			row = row_build(ROW_COPY_POINTERS, clust_index, prev_version, heap);
			entry = row_build_index_entry(row, index, heap);

			/*聚集索引上的行与ientry相同*/
			if(dtuple_datas_are_ordering_equal(ientry, entry)){
				mem_heap_free(heap);
				return TRUE;
			}
		}
		version = prev_version;
	}

	return FALSE;
}

/*为view一致性读构建一个记录版本*/
ulint row_vers_build_for_consistent_read(rec_t* rec, mtr_t* mtr, dict_index_t* index, read_view_t* view, mem_heap_t* in_heap, rec_t** old_vers)
{
	rec_t*		version;
	rec_t*		prev_version;
	dulint		prev_trx_id;
	mem_heap_t*	heap;
	mem_heap_t*	heap2;
	byte*		buf;
	ulint		err;

	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(rec), MTR_MEMO_PAGE_X_FIX)
		|| mtr_memo_contains(mtr, buf_block_align(rec), MTR_MEMO_PAGE_S_FIX));

	ut_ad(!rw_lock_own(&(purge_sys->latch), RW_LOCK_SHARED));
	ut_ad(!read_view_sees_trx_id(view, row_get_rec_trx_id(rec, index)));

	/*trx_undo_prev_version_build函数需要持有purge_sys的s-latch,用于读取undo rec*/
	rw_lock_s_lock(&(purge_sys->latch));
	version = rec;
	heap = NULL;

	for(;;){
		heap2 = heap;
		heap = mem_heap_create(1024);

		err = trx_undo_prev_version_build(rec, mtr, version, index, heap, &prev_version);
		if(heap2)
			mem_heap_free(heap2);

		if(err != DB_SUCCESS)
			break;

		if(prev_version == NULL){ /*没有更早的版本*/
			*old_vers = NULL;
			err = DB_SUCCESS;
			break;
		}

		prev_trx_id = row_get_rec_trx_id(prev_version, index);
		if (read_view_sees_trx_id(view, prev_trx_id)){ /*prev_trx_id是在事务read view的看见范围之内，可以直接返回这个事务修改的记录版本*/
			buf = mem_heap_alloc(in_heap, rec_get_size(prev_version));
			*old_vers = rec_copy(buf, prev_version);
			err = DB_SUCCESS;
			break;
		}

		version = prev_version;
	}

	mem_heap_free(heap);

	rw_lock_s_unlock(&(purge_sys->latch));

	return err;
}



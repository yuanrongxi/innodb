#include "row0umod.h"
#include "dict0dict.h"
#include "dict0boot.h"
#include "trx0undo.h"
#include "trx0roll.h"
#include "btr0btr.h"
#include "mach0data.h"
#include "row0undo.h"
#include "row0vers.h"
#include "trx0trx.h"
#include "trx0rec.h"
#include "row0row.h"
#include "row0upd.h"
#include "que0que.h"
#include "log0log.h"


/*检查前一个版本和后一个版本的记录是否是同一个trx事务操作的,并且是可回滚的*/
UNIV_INLINE ibool row_undo_mod_undo_also_prev_vers(undo_node_t* node, que_thr_t* thr, dulint* undo_no)
{
	trx_undo_rec_t*	undo_rec;
	ibool		ret;
	trx_t*		trx;

	UT_NOT_USED(thr);

	trx = node->trx;
	if(ut_dulint_cmp(node->new_trx_id, trx->id) != 0)
		return FALSE;

	/*获得new roll ptr对应的undo rec*/
	undo_rec = trx_undo_get_undo_rec_low(node->new_roll_ptr, node->heap);
	*undo_no = trx_undo_rec_get_undo_no(undo_rec);
	/*undo no处于被回滚的范围之内*/
	if(ut_dulint_cmp(trx->roll_limit, *undo_no) <= 0)
		ret = TRUE;
	else
		ret = FALSE;

	return ret;
}

/*回滚一个聚集索引上的记录修改操作*/
static ulint row_undo_mod_clust_low(undo_node_t* node, que_thr_t* thr, mtr_t* mtr, ulint mode)
{
	big_rec_t*	dummy_big_rec;
	dict_index_t*	index;
	btr_pcur_t*	pcur;
	btr_cur_t*	btr_cur;
	ulint		err;
	ibool		success;

	index = dict_table_get_first_index(node->table);

	pcur = &(node->pcur);
	btr_cur = btr_pcur_get_btr_cur(pcur);
	success = btr_pcur_restore_position(mode, pcur, mtr);
	ut_ad(success);
	if(mode == BTR_MODIFY_LEAF){ /*用update中的记录乐观式修改，update中的记录数据是在undo rec中取出来的，相当于用原来的数据覆盖新的数据*/
		err = btr_cur_optimistic_update(BTR_NO_LOCKING_FLAG| BTR_NO_UNDO_LOG_FLAG | BTR_KEEP_SYS_FLAG,
			btr_cur, node->update, node->cmpl_info, thr, mtr);
	}
	else{/*用update中的记录悲观式修改*/
		ut_ad(mode == BTR_MODIFY_TREE);

		err = btr_cur_pessimistic_update(BTR_NO_LOCKING_FLAG | BTR_NO_UNDO_LOG_FLAG | BTR_KEEP_SYS_FLAG,
			btr_cur, &dummy_big_rec, node->update, node->cmpl_info, thr, mtr);
	}

	return err;
}

/*删除一个在被rollback undo后的聚集索引记录,这个记录应该是undo后丢弃的记录,例如被回滚的update记录*/
static ulint row_undo_mod_remove_clust_low(undo_node_t* node, que_thr_t* thr, mtr_t* mtr, ulint mode)
{
	btr_pcur_t*	pcur;
	btr_cur_t*	btr_cur;
	ulint		err;
	ibool		success;

	pcur = &(node->pcur);
	btr_cur = btr_pcur_get_btr_cur(pcur);
	success = btr_pcur_restore_position(mode, pcur, mtr);
	if(!success) /*记录不存在*/
		return DB_SUCCESS;

	/*记录还不能删除，有其他事务在使用*/
	if(node->rec_type == TRX_UNDO_UPD_DEL_REC && !row_vers_must_preserve_del_marked(node->new_trx_id, mtr)){

	}
	else
		return DB_SUCCESS;

	if(mode == BTR_MODIFY_LEAF){ /*乐观式删除*/
		success = btr_cur_optimistic_delete(btr_cur, mtr);
		if(success)
			err = DB_SUCCESS;
		else
			err = DB_FAIL;
	}
	else{/*悲观式删除*/
		ut_ad(mode == BTR_MODIFY_TREE);
		btr_cur_pessimistic_delete(&err, FALSE, btr_cur, FALSE, mtr);
	}

	return err;
}

/*回滚聚集索引上的修改操作*/
static ulint row_undo_mod_clust(undo_node_t* node, que_thr_t* thr)
{
	btr_pcur_t*	pcur;
	mtr_t		mtr;
	ulint		err;
	ibool		success;
	ibool		more_vers;
	dulint		new_undo_no;

	/*检查是否可以一次回滚多个版本*/
	more_vers = row_undo_mod_undo_also_prev_vers(node, thr, &new_undo_no);
	pcur = &(node->pcur);

	mtr_start(&mtr);

	/*尝试乐观式用undo rec替换聚集索引上对应的记录*/
	err = row_undo_mod_clust_low(node, thr, &mtr, BTR_MODIFY_LEAF);
	if(err != DB_SUCCESS){
		btr_pcur_commit_specify_mtr(pcur, &mtr);
		mtr_start(&mtr);
		/*乐观替换无法进行，进行悲观替换*/
		err = row_undo_mod_clust_low(node, thr, &mtr, BTR_MODIFY_TREE);
	}

	btr_pcur_commit_specify_mtr(pcur, &mtr);
	/*老记录替换新记录成功，将新记录(node->pcur)删除*/
	if(err == DB_SUCCESS && node->rec_type == TRX_UNDO_UPD_DEL_REC){
		mtr_start(&mtr);
		err = row_undo_mod_remove_clust_low(node, thr, &mtr, BTR_MODIFY_LEAF);
		if(err != DB_SUCCESS){
			btr_pcur_commit_specify_mtr(pcur, &mtr);
			mtr_start(&mtr);
			err = row_undo_mod_remove_clust_low(node, thr, &mtr, BTR_MODIFY_TREE);
		}

		btr_pcur_commit_specify_mtr(pcur, &mtr);
	}

	/*可以进行下一个undo rec回滚*/
	node->state = UNDO_NODE_FETCH_NEXT;

	trx_undo_rec_release(node->trx, node->undo_no);
	if(more_vers && err == DB_SUCCESS){ /*设置为向前再回滚一个版本的同一个记录,这样做的目的应该是为了合并操作，避免页切换*/
		success = trx_undo_rec_reserve(node->trx, new_undo_no);
		if(success)
			node->state = UNDO_NODE_PREV_VERS;
	}

	return err;
}

/*删除一个辅助索引上的记录，被删除的记录是通过entry在辅助索引上定位到的*/
static ulint row_undo_mod_del_mark_or_remove_sec_low(undo_node_t* node, que_thr_t* thr, dict_index_t* index, dtuple_t* entry, ulint mode)
{
	ibool		found;
	btr_pcur_t	pcur;
	btr_cur_t*	btr_cur;
	ibool		success;
	ibool		old_has;
	ulint		err;
	mtr_t		mtr;
	mtr_t		mtr_vers;

	/*为什么要检查check point???一直没明白！*/
	log_free_check();

	mtr_start(&mtr);
	/*在辅助索引树上找到对应的记录*/
	found = row_search_index_entry(index, entry, mode, &pcur, &mtr);
	if(!found){
		btr_pcur_close(&pcur);
		mtr_commit(&mtr);

		return(DB_SUCCESS);
	}

	btr_cur = btr_pcur_get_btr_cur(&pcur);
	
	mtr_start(&mtr_vers);
	
	success = btr_pcur_restore_position(BTR_SEARCH_LEAF, &(node->pcur), &mtr_vers);
	ut_a(success);

	/*查询是否有其他事务在使用要删除记录的历史版本，如果有，不能进行物理删除,只能del mark*/
	old_has = row_vers_old_has_index_entry(FALSE, btr_pcur_get_rec(&(node->pcur)), &mtr_vers, index, entry);
	if(old_has){
		err = btr_cur_del_mark_set_sec_rec(BTR_NO_LOCKING_FLAG, btr_cur, TRUE, thr, &mtr);
		ut_ad(err = DB_SUCCESS);
	}
	else{ /*直接进行物理上的删除*/
		if(mode == BTR_MODIFY_LEAF){
			success = btr_cur_optimistic_delete(btr_cur, &mtr);
			if(success)
				err = DB_SUCCESS;
			else
				err = DB_FAIL;
		}
		else{
			ut_ad(mode == BTR_MODIFY_TREE);
			btr_cur_pessimistic_delete(&err, FALSE, btr_cur, TRUE, &mtr);
		}
	}

	/*关闭btree pcur,调用btr_pcur_commit_specify_mtr是因为btr_pcur_restore_position改变了pcur的状态*/
	btr_pcur_commit_specify_mtr(&(node->pcur), &mtr_vers);
	btr_pcur_close(&pcur);
	mtr_commit(&mtr);

	return err;
}

/*删除一个辅助索引上的记录，被删除的记录是通过entry在辅助索引上定位到的*/
UNIV_INLINE ulint row_undo_mod_del_mark_or_remove_sec(undo_node_t* node, que_thr_t* thr, dict_index_t* index, dtuple_t* entry)
{
	ulint	err;

	err = row_undo_mod_del_mark_or_remove_sec_low(node, thr, index, entry, BTR_MODIFY_LEAF);
	if (err == DB_SUCCESS)
		return(err);

	return row_undo_mod_del_mark_or_remove_sec_low(node, thr, index, entry, BTR_MODIFY_TREE);
}

/*取消辅助索引上的记录删除(del mark)标识*/
static void row_undo_mod_del_unmark_sec(undo_node_t* node, que_thr_t* thr, dict_index_t* index, dtuple_t* entry)
{
	btr_pcur_t	pcur;
	btr_cur_t*	btr_cur;
	ulint		err;
	ibool		found;
	mtr_t		mtr;
	char           	err_buf[1000];

	UT_NOT_USED(node);

	log_free_check();
	mtr_start(&mtr);

	found = row_search_index_entry(index, entry, BTR_MODIFY_LEAF, &pcur, &mtr);
	if(!found){
		fprintf(stderr, "InnoDB: error in sec index entry del undo in\n"
			"InnoDB: index %s table %s\n", index->name, index->table->name);

		dtuple_sprintf(err_buf, 900, entry);
		fprintf(stderr, "InnoDB: tuple %s\n", err_buf);

		rec_sprintf(err_buf, 900, btr_pcur_get_rec(&pcur));
		fprintf(stderr, "InnoDB: record %s\n", err_buf);

		trx_print(err_buf, thr_get_trx(thr));
		fprintf(stderr, "%s\nInnoDB: Make a detailed bug report and send it\n", err_buf);
		fprintf(stderr, "InnoDB: to mysql@lists.mysql.com\n");
	}
	else{ /*取消del mark标识*/
		btr_cur = btr_pcur_get_btr_cur(&pcur);
		err = btr_cur_del_mark_set_sec_rec(BTR_NO_LOCKING_FLAG, btr_cur, FALSE, thr, &mtr);
		ut_ad(err == DB_SUCCESS);
	}

	btr_pcur_close(&pcur);
	mtr_commit(&mtr);
}

/*在辅助索引上回滚一个UPD_DEL类型的操作*/
static ulint row_undo_mod_upd_del_sec(undo_node_t* node, que_thr_t* thr)
{
	mem_heap_t*	heap;
	dtuple_t*	entry;
	dict_index_t*	index;
	ulint		err;

	heap = mem_heap_create(1024);
	while(node->index != NULL){
		index = node->index;
		entry = row_build_index_entry(node->row, index, heap);
		err = row_undo_mod_del_mark_or_remove_sec(node, thr, index, entry);
		if(err != DB_SUCCESS){
			mem_heap_free(heap);
			return err;
		}

		node->index = dict_table_get_next_index(node->index);
	}
	mem_heap_free(heap);

	return err;
}

/*在辅助索引上回滚一个DEL_MARK类型的操作*/
static ulint row_undo_mod_del_mark_sec(undo_node_t* node, que_thr_t* thr)
{
	mem_heap_t*	heap;
	dtuple_t*	entry;
	dict_index_t*	index;

	heap = mem_heap_create(1024);
	while (node->index != NULL) {
		index = node->index;
		entry = row_build_index_entry(node->row, index, heap);
		row_undo_mod_del_unmark_sec(node, thr, index, entry);

		node->index = dict_table_get_next_index(node->index);
	}

	mem_heap_free(heap);	

	return DB_SUCCESS;
}

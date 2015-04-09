#include "row0uins.h"
#include "dict0dict.h"
#include "dict0boot.h"
#include "dict0crea.h"
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
#include "ibuf0ibuf.h"
#include "log0log.h"

/*删除node定位到对应的聚集索引的记录*/
static ulint row_undo_ins_remove_clust_rec(undo_node_t* node, que_thr_t* thr)
{
	btr_cur_t*	btr_cur;		
	ibool		success;
	ulint		err;
	ulint		n_tries	= 0;
	mtr_t		mtr;

	UT_NOT_USED(thr);

	mtr_start(&mtr);

	success = btr_pcur_restore_position(BTR_MODIFY_LEAF, &(node->pcur), &mtr);
	ut_a(success);

	if (ut_dulint_cmp(node->table->id, DICT_INDEXES_ID) == 0){ /*系统的索引表SYS_INDEXES,删除一个索引，必须将对应的索引树删除*/
		dict_drop_index_tree(btr_pcur_get_rec(&(node->pcur)), &mtr);
		mtr_commit(&mtr);

		mtr_start(&mtr);
		success = btr_pcur_restore_position(BTR_MODIFY_LEAF, &(node->pcur), &mtr);
		ut_a(success);
	}

	/*在btree树上将记录从物理上删除*/
	btr_cur = btr_pcur_get_btr_cur(&(node->pcur));
	success = btr_cur_optimistic_delete(btr_cur, &mtr);

	btr_pcur_commit_specify_mtr(&(node->pcur), &mtr);
	if(success){ /*删除undo 与roll array的对应关系*/
		trx_undo_rec_release(node->trx, node->undo_no);
		return DB_SUCCESS;
	}

	/*乐观式删除失败，需要进行悲观式删除,涉及到表空间的IO操作*/
retry:
	mtr_start(&mtr);
	success = btr_pcur_restore_position(BTR_MODIFY_TREE, &(node->pcur), &mtr);
	ut_a(success);
	/*悲观式删除，涉及到page的删除和表空间的调整*/
	btr_cur_pessimistic_delete(&err, FALSE, btr_cur, TRUE, &mtr);
	if(err == DB_OUT_OF_FILE_SPACE && n_tries < BTR_CUR_RETRY_DELETE_N_TIMES){
		btr_pcur_commit_specify_mtr(&(node->pcur), &mtr);
		n_tries ++;
		os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);

		goto retry;
	}

	btr_pcur_commit_specify_mtr(&(node->pcur), &mtr);
	trx_undo_rec_release(node->trx, node->undo_no);

	return err;
}

/*undo操作触发删除辅助索引上的记录*/
static ulint row_undo_ins_remove_sec_low(ulint mode, dict_index_t* index, dtuple_t* entry, que_thr_t* thr)
{
	btr_pcur_t	pcur;		
	btr_cur_t*	btr_cur;
	ibool		found;
	ibool		success;
	ulint		err;
	mtr_t		mtr;

	UT_NOT_USED(thr);
	/*check point检查？*/
	log_free_check();

	mtr_start(&mtr);
	/*通过entry在辅助索引上定位到pcur位置*/
	found = row_search_index_entry(index, entry, mode, &pcur, &mtr);
	if(!found){ /*二级索引不存在对应的记录*/
		btr_pcur_close(&pcur);
		mtr_commit(&mtr);

		return DB_SUCCESS;
	}

	btr_cur = btr_pcur_get_btr_cur(&pcur);
	if(mode == BTR_MODIFY_LEAF){
		success = btr_cur_optimistic_delete(btr_cur, &mtr);
		if(success)
			err = DB_SUCCESS;
		else
			err = DB_FAIL;
	}
	else{/*进行悲观式删除*/
		ut_ad(mode == BTR_MODIFY_TREE);
		btr_cur_pessimistic_delete(&err, FALSE, btr_cur, TRUE, &mtr);
	}

	btr_pcur_close(&pcur);
	mtr_commit(&mtr);

	return err;
}

/*根据index entry删除一个辅助索引记录，先尝试乐观删除，在尝试悲观删除*/
static ulint row_undo_ins_remove_sec(dict_index_t* index, dtuple_t* entry, que_thr_t* thr)
{
	ulint	err;
	ulint	n_tries	= 0;

	/*乐观式删除*/
	err = row_undo_ins_remove_sec_low(BTR_MODIFY_LEAF, index, entry, thr);
	if(err == DB_SUCCESS)
		return err;

	/*悲观式删除，会进行重试*/
retry:
	err = row_undo_ins_remove_sec_low(BTR_MODIFY_TREE, index, entry, thr);
	if(err != DB_SUCCESS && n_tries < BTR_CUR_RETRY_DELETE_N_TIMES){ /*删除失败，进行等待一个BTR_CUR_RETRY_SLEEP_TIME时刻后再重试*/
		n_tries ++;
		os_thread_sleep(BTR_CUR_RETRY_SLEEP_TIME);
		goto retry;
	}

	return err;
}

/*从undo insert rec记录中解析,并将信息存入node中*/
static void row_undo_ins_parse_undo_rec(undo_node_t* node, que_thr_t* thr)
{
	dict_index_t*	clust_index;
	byte*		ptr;
	dulint		undo_no;
	dulint		table_id;
	ulint		type;
	ulint		dummy;
	ibool		dummy_extern;

	ut_ad(node & thr);

	/*从undo rec中读取undo no、type、table id等信息*/
	ptr = trx_undo_rec_get_pars(node->undo_rec, &type, &dummy, &dummy_extern, &undo_no, &table_id);
	ut_ad(type == TRX_UNDO_INSERT_REC);
	node->rec_type = type;
	/*通过table id获得表对象*/
	node->table = dict_table_get_on_id(table_id, node->trx);
	if(node->table == NULL)
		return;

	clust_index = dict_table_get_first_index(node->table);
	/*解析undo rec并获得一个tuple逻辑记录对象(node->ref)*/
	ptr = trx_undo_rec_get_row_ref(ptr, clust_index, &(node->ref), node->heap);
}

/*对insert操作的回滚*/
ulint row_undo_ins(undo_node_t* node, que_thr_t* thr)
{
	dtuple_t*	entry;
	ibool		found;
	ulint		err;

	ut_ad(node && thr);
	ut_ad(node->state == UNDO_NODE_INSERT);

	row_undo_ins_parse_undo_rec(node, thr);
	/*在聚集索引上找到对应的记录位置*/
	if(node->table == NULL)
		found = FALSE;
	else
		found = row_undo_search_clust_to_pcur(node, thr);

	if(!found){
		trx_undo_rec_release(node->trx, node->undo_no);
		return(DB_SUCCESS);
	}

	/*先删除所有辅助索引上对应的记录*/
	node->index = dict_table_get_next_index(dict_table_get_first_index(node->table));
	while(node->index != NULL){
		entry = row_build_index_entry(node->row, node->index, node->heap);
		err = row_undo_ins_remove_sec(node->index, entry, thr);
		if(err != DB_SUCCESS)
			return err;

		node->index = dict_table_get_next_index(node->index);
	}

	/*删除聚集索引上的记录,在前面已经做了定位，位置在node->pcur中*/
	err = row_undo_ins_remove_clust_rec(node, thr);
}
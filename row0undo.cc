#include "row0undo.h"
#include "fsp0fsp.h"
#include "mach0data.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0roll.h"
#include "trx0undo.h"
#include "trx0purge.h"
#include "trx0rec.h"
#include "que0que.h"
#include "row0row.h"
#include "row0uins.h"
#include "row0umod.h"
#include "srv0srv.h"

/*创建一个undo query graph node*/
undo_node_t* row_undo_node_create(trx_t* trx, que_thr_t* parent, mem_heap_t* heap)
{
	undo_node_t* undo;
	ut_ad(trx && parent && heap);

	undo = mem_heap_alloc(heap, sizeof(undo_node_t));
	undo->common.type = QUE_NODE_UNDO;
	undo->common.parent = parent;
	undo->state = UNDO_NODE_FETCH_NEXT;
	undo->trx = trx;

	btr_pcur_init(&(undo->pcur));
	undo->heap = mem_heap_create(256);

	return undo;
}

/*通过node对应的参考记录信息和roll ptr定位到聚集索引对应的rec记录对象*/
ibool row_undo_search_clust_to_pcur(undo_node_t* node, que_thr_t* thr)
{
	dict_index_t*	clust_index;
	ibool		found;
	mtr_t		mtr;
	ibool		ret;
	rec_t*		rec;

	UT_NOT_USED(thr);

	mtr_start(&mtr);

	clust_index = dict_table_get_first_index(node->table);
	/*在聚集索引上查找node->ptr对应索引树的位置*/
	found = row_search_on_row_ref(&(node->pcur), BTR_MODIFY_LEAF, node->table, node->ref, &mtr);
	rec = btr_pcur_get_rec(&(node->pcur));

	/*没找到对应记录或者roll ptr不相同,表示没有对应的UNDO记录*/
	if(!found || 0 != ut_dulint_cmp(node->roll_ptr,row_get_rec_roll_ptr(rec, clust_index))){
		ret = FALSE;
	}
	else{
		/*构建一个行记录对象*/
		node->row = row_build(ROW_COPY_DATA, rec, clust_index, node->heap);
		btr_pcur_store_position(&(node->pcur), &mtr);
		ret = TRUE;
	}
	/*对mtr的commit并改变pcur的状态为空闲状态*/
	btr_pcur_commit_specify_mtr(&(node->pcur), &mtr);

	return ret;
}

/*对node指定的undo rec进行回滚操作*/
static ulint row_undo(undo_node_t* node, que_thr_t* thr)
{
	ulint	err;
	trx_t*	trx;
	dulint	roll_ptr;

	ut_ad(node && thr);

	trx = node->trx;
	if(node->state == UNDO_NODE_FETCH_NEXT){
		/*回滚大于roll_limit的undo rec*/
		node->undo_rec = trx_roll_pop_top_rec_of_trx(trx, trx->roll_limit, &roll_ptr, node->heap);
		if(node->undo_rec != NULL){ /*没有可以undo的记录,说明回滚完成*/
			thr->run_node = que_node_get_parent(node);
			return DB_SUCCESS;
		}

		/*将roll ptr和undo_no设置到node当中,在后面使用*/
		node->roll_ptr = roll_ptr;
		node->undo_no = trx_undo_rec_get_undo_no(node->undo_rec);
		if(trx_undo_roll_ptr_is_insert(roll_ptr))
			node->state = UNDO_NODE_INSERT;
		else
			node->state = UNDO_NODE_MODIFY;
	}
	else if(node->state == UNDO_NODE_PREV_VERS){ /*进行向前版本的undo*/
		roll_ptr = node->new_roll_ptr;
		/*将roll ptr指向的undo rec拷贝出来*/
		node->undo_rec = trx_undo_get_undo_rec_low(roll_ptr, node->heap);
		node->roll_ptr = roll_ptr;
		node->undo_no = trx_undo_rec_get_undo_no(node->undo_rec);
		if(trx_undo_roll_ptr_is_insert(roll_ptr))
			node->state = UNDO_NODE_INSERT;
		else
			node->state = UNDO_NODE_MODIFY;
	}

	if(node->state == UNDO_NODE_INSERT){ /*回滚insert,因为insert操作只有1条undo rec,索引回滚完成后直接进行下一条undo rec的回滚*/
		err = row_undo_ins(node, thr);
		node->state = UNDO_NODE_FETCH_NEXT;
	}
	else{ /*对update回滚*/
		ut_ad(node->state == UNDO_NODE_MODIFY);
		err = row_undo_mod(node, thr);
	}
	/*node->pcur是在row_undo_ins或row_undo_mod中打开的*/
	btr_pcur_close(&(node->pcur));
	mem_heap_empty(node->heap);
	thr->run_node = node;

	return err;
}

/*undo任务(query graph node)执行,这个任务是有trx_rollback函数派发的*/
que_thr_t* row_undo_step(que_thr_t* thr)
{
	ulint			err;
	undo_node_t*	node;
	trx_t*			trx;

	ut_ad(thr);

	/*唤醒了一个mysql暂停的线程，计数器需要+1*/
	srv_activity_count ++;

	trx = thr_get_trx(thr);
	node = thr->run_node;
	ut_ad(que_node_get_type(node) == QUE_NODE_UNDO);

	err = row_undo(node, thr);
	trx->error_state = err;
	if(err != DB_SUCCESS){
		/*SQL error detected*/
		fprintf(stderr, "InnoDB: Fatal error %lu in rollback.\n", err);
		if (err == DB_OUT_OF_FILE_SPACE) {
			fprintf(stderr, "InnoDB: Error 13 means out of tablespace.\n"
				"InnoDB: Consider increasing your tablespace.\n");

			exit(1);			
		}
		ut_a(0);

		return NULL;
	}

	return thr;
}






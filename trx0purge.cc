#include "trx0purge.h"
#include "fsp0fsp.h"
#include "mach0data.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0roll.h"
#include "read0read.h"
#include "fut0fut.h"
#include "que0que.h"
#include "row0purge.h"
#include "row0upd.h"
#include "trx0rec.h"
#include "srv0que.h"
#include "os0thread.h"

trx_purge_t*	purge_sys = NULL;
trx_undo_rec_t  trx_purge_dummy_rec;

/*检查trx_id对应的事务是否在系统中，如果如果在，返回TRUE，否则返回FALSE*/
ibool trx_purge_update_undo_must_exist(dulint trx_id)
{
	ut_ad(rw_lock_own(&(purge_sys->latch), RW_LOCK_SHARED));

	if(!read_view_sees_trx_id(purge_sys->view, trx_id))
		return TRUE;

	return FALSE;
}

/*在purge过程中，保存undo log对应的信息(trx_no, undo_no)*/
static trx_undo_inf_t* trx_purge_arr_store_info(dulint trx_no, dulint undo_no)
{
	trx_undo_inf_t*	cell;
	trx_undo_arr_t*	arr;
	ulint		i;

	arr = purge_sys->arr;
	for(i = 0; ; i++){
		cell = trx_undo_arr_get_nth_info(arr, i);
		if(!cell->in_use){
			cell->undo_no = undo_no;
			cell->trx = trx_no;
			cell->in_use = TRUE;

			arr->n_used ++;

			return cell;
		}
	}

	return NULL;
}

/*在purge过程中将一个undo log状态信息从cell array中删除*/
UNIV_INLINE void trx_purge_arr_remove_info(trx_undo_inf_t* cell)
{
	trx_undo_arr_t* arr;

	arr = purge_sys->arr;
	cell->in_use = FALSE;
	ut_ad(arr->n_used > 0);
	arr->n_used  --;
}

/*在purge array中查找最大的(trx_no, undo_no)对*/
static void trx_purge_arr_get_biggest(trx_undo_arr_t* arr, dulint* trx_no, dulint* undo_no)
{
	trx_undo_inf_t*	cell;
	dulint		pair_trx_no;
	dulint		pair_undo_no;
	int		trx_cmp;
	ulint		n_used;
	ulint		i;
	ulint		n;

	n  = 0;
	n_used = arr->n_used;
	pair_trx_no = ut_dulint_zero;
	pair_undo_no = ut_dulint_zero;

	for(i = 0; ; i++){
		cell = trx_undo_arr_get_nth_info(arr, i);
		if(cell->is_use){
			n ++;
			trx_cmp = ut_dulint_cmp(cell->trx_no, pair_trx_no);
			if(trx_cmp > 0 ||(trx_cmp == 0 && ut_dulint_cmp(cell->trx_no, pair_trx_no) >= 0)){
				pair_trx_no = cell->trx_no;
				pair_undo_no = cell->undo_no;
			}
		}

		if(n == n_used){
			*trx_no = pair_trx_no;
			*undo_no = pair_undo_no;

			return;
		}
	}
}

/*创建一个query graph,并在这个query graph执行purge?!*/
static que_t* trx_purge_graph_build()
{
	mem_heap_t*	heap;
	que_fork_t*	fork;
	que_thr_t*	thr;

	heap = mem_heap_create(512);
	fork = que_fork_create(NULL, NULL, QUE_FORK_PURGE, heap);
	fork->trx = purge_sys->trx;

	thr = que_thr_create(fork, heap);

	return fork;
}

/*创建全局的purge system*/
void trx_purge_sys_create()
{
	com_endpoint_t*	com_endpoint;

	ut_ad(mutex_own(&kernel_mutex));

	purge_sys = mem_alloc(sizeof(trx_purge_t));
	purge_sys->state = TRX_STOP_PURGE;
	purge_sys->n_pages_handled = 0;
	purge_sys->purge_trx_no = ut_dulint_zero;
	purge_sys->purge_undo_no = ut_dulint_zero;
	purge_sys->next_stored = FALSE;

	rw_lock_create(&(purge_sys->purge_is_running));
	rw_lock_set_level(&(purge_sys->purge_is_running), SYNC_PURGE_IS_RUNNING);

	rw_lock_create(&(purge_sys->latch));
	rw_lock_set_level(&(purge_sys->latch), SYNC_PURGE_LATCH);

	mutex_create(&(purge_sys->mutex));
	mutex_set_level(&(purge_sys->mutex), SYNC_PURGE_SYS);

	purge_sys->heap = mem_heap_create(256);
	purge_sys->arr = trx_undo_arr_create();

	com_endpoint = (com_endpoint_t*)purge_sys;
	purge_sys->sess = sess_open(com_endpoint, (byte*)"purge_system", 13);
	purge_sys->trx = purge_sys->sess->trx;
	purge_sys->trx->type = TRX_PURGE;

	ut_a(trx_start_low(purge_sys->trx, ULINT_UNDEFINED));
	purge_sys->query = trx_purge_graph_build();
	purge_sys->view = read_view_oldest_copy_or_open_new(NULL, purge_sys->heap);
}

/*向rseg header的histroy list加入update undo log*/
void trx_purge_add_update_undo_to_history(trx_t* trx, page_t* undo_page, mtr_t* mtr)
{
	trx_undo_t*	undo;
	trx_rseg_t*	rseg;
	trx_rsegf_t*	rseg_header;
	trx_usegf_t*	seg_header;
	trx_ulogf_t*	undo_header;
	trx_upagef_t*	page_header;
	ulint		hist_size;

	undo = trx->update_undo;
	ut_ad(undo);

	rseg = undo->rseg;
	ut_ad(mutex_own(&(rseg->mutex)));

	rseg_header = trx_rsegf_get(rseg->space, rseg->page_no, mtr);

	undo_header = undo_page + undo->hdr_offset;
	seg_header  = undo_page + TRX_UNDO_SEG_HDR;
	page_header = undo_page + TRX_UNDO_PAGE_HDR;
	if(undo->state != TRX_UNDO_CACHED){
		if(undo->id >= TRX_RSEG_N_SLOTS){
			fprintf(stderr, "InnoDB: Error: undo->id is %lu\n", undo->id);
			ut_a(0);
		}

		/*去掉undo与rseg slots的关联*/
		trx_rsegf_set_nth_undo(rseg_header, undo->id, FIL_NULL, mtr);

		/*从新更新rseg header的TRX_RSEG_HISTORY_SIZE*/
		hist_size = mtr_read_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, mtr);
		ut_ad(undo->size == flst_get_len(seg_header + TRX_UNDO_PAGE_LIST, mtr));
		mlog_write_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE, hist_size + undo->size, MLOG_4BYTES, mtr);
	}
	/*将undo log插入到rseg header的history list的头上*/
	flst_add_first(rseg_header + TRX_RSEG_HISTORY, undo_header + TRX_UNDO_HISTORY_NODE, mtr);
	/*更新undo log header的trx_id*/
	mlog_write_dulint(undo_header + TRX_UNDO_TRX_NO, trx->no, MLOG_8BYTES, mtr);

	if(!undo->del_marks)
		mlog_write_ulint(undo_header + TRX_UNDO_DEL_MARKS, FALSE, MLOG_2BYTES, mtr);

	if(rseg->last_page_no == FIL_NULL){
		rseg->last_page_no = undo->hdr_page_no;
		rseg->last_offset = undo->hdr_offset;
		rseg->last_trx_no = trx->no;
		rseg->last_del_marks = undo->del_marks;
	}
}

/*purge过程释放一整个在history list中undo log segment*/
static void trx_purge_free_segment(trx_rseg_t* rseg, fil_addr_t hdr_addr, ulint n_removed_logs)
{
	page_t*		undo_page;
	trx_rsegf_t*	rseg_hdr;
	trx_ulogf_t*	log_hdr;
	trx_usegf_t*	seg_hdr;
	ibool		freed;
	ulint		seg_size;
	ulint		hist_size;
	ibool		marked	= FALSE;
	mtr_t		mtr;

	ut_ad(mutex_own(&(purge_sys->mutex)));

loop:
	mtr_start(&mtr);
	mutex_enter(&(rseg->mutex));

	rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);

	undo_page = trx_undo_page_get(rseg->space, hdr_addr.page, &mtr);
	seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
	log_hdr = undo_page + hdr_addr.boffset;

	/*将undo log header设置为del mark*/
	if(!marked){
		mlog_write_ulint(log_hdr + TRX_UNDO_DEL_MARKS, FALSE, MLOG_2BYTES, &mtr);
		marked = TRUE;
	}

	/*对表空间对应的undo log segment的释放，不释放头页*/
	freed = fseg_free_step_not_header(seg_hdr + TRX_UNDO_FSEG_HEADER, &mtr);
	if(!freed){ /*如果失败，重试，直到成功为止*/
		mutex_exit(&(rseg->mutex));
		mtr_commit(&mtr);

		goto loop;
	}

	/*解除与rseg header中history list的关系*/
	seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST, &mtr);
	flst_cut_end(rseg_hdr + TRX_RSEG_HISTORY, log_hdr + TRX_UNDO_HISTORY_NODE, n_removed_logs, &mtr);

	freed = FALSE;
	while(!freed){ /*释放undo log segment的header page*/
		freed = fseg_free_step(seg_hdr + TRX_UNDO_FSEG_HEADER, &mtr);
	}

	/*更改history list中的页数量*/
	hist_size = mtr_read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, &mtr);
	ut_ad(hist_size >= seg_size);
	mlog_write_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, hist_size - seg_size, MLOG_4BYTES, &mtr);
	
	ut_ad(rseg->curr_size >= seg_size);
	rseg->curr_size -= seg_size;

	mutex_exit(&(rseg->mutex));
	mtr_commit(&mtr);
}

/*删除rollback segment中多余的history list中的undo log,所有小于limit_undo_no的undo log会被删除*/
static void trx_purge_truncate_rseg_history(trx_rseg_t* rseg, dulint limit_trx_no, dulint limit_undo_no)
{
	fil_addr_t	hdr_addr;
	fil_addr_t	prev_hdr_addr;
	trx_rsegf_t*	rseg_hdr;
	page_t*		undo_page;
	trx_ulogf_t*	log_hdr;
	trx_usegf_t*	seg_hdr;
	int		cmp;
	ulint		n_removed_logs	= 0;
	mtr_t		mtr;

	ut_ad(mutex_own(&(purge_sys->mutex)));

	mtr_start(&mtr);
	mutex_enter(&(rseg->mutex));

	rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);
	hdr_addr = trx_purge_get_log_from_hist(flst_get_last(rseg_hdr + TRX_RSEG_HISTORY, &mtr));

loop:
	/*undo log header page不存在*/
	if(hdr_addr.page == FIL_NULL){
		mutex_exit(&(rseg->mutex));
		mtr_commit(&mtr);
		return ;
	}

	undo_page = trx_undo_page_get(rseg->space, hdr_addr.page, &mtr);
	log_hdr = undo_page + hdr_addr.boffset;

	cmp = ut_dulint_cmp(mach_read_from_8(log_hdr + TRX_UNDO_TRX_NO), limit_trx_no);
	if(cmp == 0) /*移除掉对应的hdr page中所有小于limit_undo_no的undo log rec*/
		trx_undo_truncate_start(rseg, rseg->space, hdr_addr.page, hdr_addr.boffset, limit_undo_no);

	if(cmp >= 0){ /*删除history list对应的节点,在trx_undo_truncate_start加入的节点？？*/
		flst_truncate_end(rseg_hdr + TRX_RSEG_HISTORY, log_hdr + TRX_UNDO_HISTORY_NODE, n_removed_logs, &mtr);
		mutex_exit(&(rseg->mutex));	
		mtr_commit(&mtr);
		return ;
	}

	prev_hdr_addr = trx_purge_get_log_from_hist(flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));
	n_removed_logs++;

	seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
	/*后面没有undo log*/
	if((mach_read_from_2(seg_hdr + TRX_UNDO_STATE) == TRX_UNDO_TO_PURGE) && (mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0)){
		mutex_exit(&(rseg->mutex));	
		mtr_commit(&mtr);
		/*可以释放整个undo log段*/
		trx_purge_free_segment(rseg, hdr_addr, n_removed_logs);
		n_removed_logs = 0;
	}
	else{
		mutex_exit(&(rseg->mutex));	
		mtr_commit(&mtr);
	}

	mtr_start(&mtr);
	mutex_enter(&(rseg->mutex));	

	rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);
	hdr_addr = prev_hdr_addr;

	goto loop;
}

/*删除所有rollback segment的history list中所有不必要的undo log*/
static void trx_purge_truncate_history(trx_rseg_t* rseg, dulint limit_trx_no, dulint limit_undo_no)
{
	trx_rseg_t*	rseg;
	dulint		limit_trx_no;
	dulint		limit_undo_no;

	ut_ad(mutex_own(&(purge_sys->mutex)));

	/*获取limit undo_no/tx_id*/
	trx_purge_arr_get_biggest(purge_sys->arr, &limit_trx_no, &limit_undo_no);
	if(ut_dulint_cmp(limit_trx_no, ut_dulint_zero) == 0){
		limit_trx_no = purge_sys->purge_trx_no;
		limit_undo_no = purge_sys->purge_undo_no;
	}

	if (ut_dulint_cmp(limit_trx_no, purge_sys->view->low_limit_no) >= 0) {
		limit_trx_no = purge_sys->view->low_limit_no;
		limit_undo_no = ut_dulint_zero;
	}

	ut_ad((ut_dulint_cmp(limit_trx_no, purge_sys->view->low_limit_no) <= 0));

	rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);
	while(rseg){
		trx_purge_truncate_rseg_history(rseg, limit_trx_no, limit_undo_no);
		rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
	}
}

UNIV_INLINE ibool trx_purge_truncate_if_arr_empty()
{
	ut_ad(mutex_own(&(purge_sys->mutex)));

	if(purge_sys->arr->n_used == 0){ /*arr中是空的，必须删除掉小于last undo no的undo log*/
		trx_purge_truncate_history();
		return TRUE;
	}

	return FALSE;
}

/*更新rseg中最近未被purge的(page no, offset, trx_no, undo_no),就是last_page对应undo log在history list的前一个undo log的对应的数据*/
static void trx_purge_rseg_get_next_history_log(trx_rseg_t* rseg)
{
	page_t* 	undo_page;
	trx_ulogf_t*	log_hdr;
	trx_usegf_t*	seg_hdr;
	fil_addr_t	prev_log_addr;
	dulint		trx_no;
	ibool		del_marks;
	mtr_t		mtr;

	ut_ad(mutex_own(&(purge_sys->mutex)));
	mutex_enter(&(rseg->mutex));

	ut_ad(rseg->last_page_no != FIL_NULL);

	purge_sys->purge_trx_no = ut_dulint_add(rseg->last_trx_no, 1);
	purge_sys->purge_undo_no = ut_dulint_zero;
	purge_sys->next_stored = FALSE;

	mtr_start(&mtr);

	undo_page = trx_undo_page_get_s_latched(rseg->space, rseg->last_page_no, &mtr);
	log_hdr = undo_page + rseg->last_offset;
	seg_hdr = undo_page + TRX_UNDO_SEG_HDR;

	/*最后的undo log header在undo log segment的头页中,这个undo log segment不能复用*/
	if(mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0 && mach_read_from_2(seg_hdr + TRX_UNDO_STATE) == TRX_UNDO_TO_PURGE)
		purge_sys->n_pages_handled ++;

	prev_log_addr = trx_purge_get_log_from_hist(flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));
	if(prev_log_addr.page == NULL){ /*history list只有1个单元节点*/
		rseg->last_page_no = FIL_NULL;
		mutex_exit(&(rseg->mutex));
		mtr_commit(&mtr);
		return ;
	}

	mutex_exit(&(rseg->mutex));
	mtr_commit(&mtr);

	log_hdr = trx_undo_page_get_s_latched(rseg->space, prev_log_addr.page, &mtr) + prev_log_addr.boffset;
	trx_no = mach_read_from_8(log_hdr + TRX_UNDO_TRX_NO);
	del_marks = mach_read_from_2(log_hdr + TRX_UNDO_DEL_MARKS);
	mtr_commit(&mtr);

	mutex_enter(&(rseg->mutex));
	/*更新rseg中的last信息*/
	rseg->last_page_no = prev_log_addr.page;
	rseg->last_offset = prev_log_addr.boffset;
	rseg->last_trx_no = trx_no;
	rseg->last_del_marks = del_marks;

	mutex_exit(&(rseg->mutex));
}

/*确定下次purge的起始位置*/
static void trx_purge_choose_next_log()
{
	trx_undo_rec_t*	rec;
	trx_rseg_t*	rseg;
	trx_rseg_t*	min_rseg;
	dulint		min_trx_no;
	ulint		space;
	ulint		page_no;
	ulint		offset;
	mtr_t		mtr;

	ut_ad(mutex_own(&(purge_sys->mutex)));
	ut_ad(purge_sys->next_stored == FALSE);

	rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);
	min_trx_no = ut_dulint_max;
	min_rseg = NULL;

	while(rseg){
		mutex_enter(&(rseg->mutex));

		if(rseg->last_page_no != FIL_NULL){
			if(min_rseg == NULL || (ut_dulint_cmp(min_trx_no, rseg->last_trx_no) > 0)){ /*保存min trx对应的rseg等信息*/
				min_rseg = rseg;
				min_trx_no = rseg->last_trx_no;

				space = rseg->space;
				ut_a(space == 0); /* We assume in purge of externally stored fields that space id == 0 */

				page_no = rseg->last_page_no;
				offset = rseg->last_offset;
			}
		}

		mutex_exit(&(rseg->mutex));
		rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
	}

	if(min_rseg == NULL)
		return ;

	mtr_start(&mtr);
	if(!min_rseg->last_del_marks)
		rec = &trx_purge_dummy_rec;
	else{
		rec = trx_undo_get_first_rec(space, page_no, offset, RW_S_LATCH, &mtr);
		if(rec == NULL)
			rec = &trx_purge_dummy_rec;
	}

	purge_sys->next_stored = TRUE;
	purge_sys->rseg = min_rseg;

	purge_sys->hdr_page_no = page_no;
	purge_sys->hdr_offset = offset;

	purge_sys->purge_trx_no = min_trx_no;

	if(rec == &trx_purge_dummy_rec){
		purge_sys->purge_undo_no = ut_dulint_zero;
		purge_sys->page_no = page_no;
		purge_sys->offset = 0;
	}
	else{ /*purge起始的位置信息*/
		purge_sys->purge_undo_no = trx_undo_rec_get_undo_no(rec);
		purge_sys->page_no = buf_frame_get_page_no(rec);
		purge_sys->offset = rec - buf_frame_align(rec);
	}

	mtr_commit(&mtr);
}

/**/
static trx_undo_rec_t* trx_purge_get_next_rec(mem_heap_t* heap)
{
	trx_undo_rec_t*	rec;
	trx_undo_rec_t*	rec_copy;
	trx_undo_rec_t*	rec2;
	trx_undo_rec_t*	next_rec;
	page_t* 	undo_page;
	page_t* 	page;
	ulint		offset;
	ulint		page_no;
	ulint		space;
	ulint		type;
	ulint		cmpl_info;
	mtr_t		mtr;

	ut_ad(mutex_own(&(purge_sys->mutex)));
	ut_ad(purge_sys->next_stored);

	space = purge_sys->rseg->space;
	page_no = purge_sys->page_no;
	offset = purge_sys->offset;

	if(offset == 0){
		/* It is the dummy undo log record, which means that there is no need to purge this undo log */
		trx_purge_rseg_get_next_history_log(purge_sys->rseg);
		trx_purge_choose_next_log();

		return &trx_purge_dummy_rec;
	}
	
	mtr_start(&mtr);

	/*确定purge的开始位置*/
	undo_page = trx_undo_page_get_s_latched(space, page_no, &mtr);
	rec = undo_page + offset;

	rec2 = rec;
	for(;;){
		next_rec = trx_undo_page_get_next_rec(rec2, purge_sys->hdr_page_no, purge_sys->hdr_offset);
		if(next_rec == NULL){ /*跨页的下一条记录*/
			rec2 = trx_undo_get_next_rec(rec2, purge_sys->hdr_page_no, purge_sys->hdr_offset, &mtr);
			break;
		}

		rec2 = next_rec;
		type = trx_undo_rec_get_type(rec2);
		if(type == TRX_UNDO_DEL_MARK_REC) /*有一个del mark记录*/
			break;

		cmpl_info = trx_undo_rec_get_cmpl_info(rec2);
		if(trx_undo_rec_get_extern_storage(rec2))
			break;

		if ((type == TRX_UNDO_UPD_EXIST_REC) && !(cmpl_info & UPD_NODE_NO_ORD_CHANGE))
			break;
	}

	if(rec2 == NULL){
		mtr_commit(&mtr);
		trx_purge_rseg_get_next_history_log(purge_sys->rseg);
		trx_purge_choose_next_log();

		mtr_start(&mtr);
		undo_page = trx_undo_page_get_s_latched(space, page_no, &mtr);
		rec = undo_page + offset;
	}
	else{/*确定下一次合并的结束位置*/
		page = buf_frame_align(rec2);

		purge_sys->purge_undo_no = trx_undo_rec_get_undo_no(rec2);
		purge_sys->page_no = buf_frame_get_page_no(page);
		purge_sys->offset = rec2 - page;
		/* We advance to a new page of the undo log: */
		if (undo_page != page)
			purge_sys->n_pages_handled++;
	}

	rec_copy = trx_undo_rec_copy(rec, heap);
	mtr_commit(&mtr);
	return rec_copy;
}

/*获得purge对象中下一条可以purge的undo log rec对象*/
trx_undo_rec_t* trx_purge_fetch_next_rec(dulint* roll_ptr, trx_undo_inf_t** cell, mem_heap_t* heap)
{
	trx_undo_rec_t*	undo_rec;

	mutex_enter(&(purge_sys->mutex));
	if(purge_sys->state == TRX_STOP_PURGE){ /*purge处于STOP状态*/
		trx_purge_truncate_if_arr_empty(); /*删除掉purge_sys->arr中不必要的数据*/
		mutex_exit(&(purge_sys->mutex));
		return NULL;
	}

	if(!purge_sys->next_stored){ /*没有确定开始purge的位置*/
		trx_purge_choose_next_log(); /*尝试计算一次*/

		if(!purge_sys->next_stored){ /*还是没有可以purge的数据，将purge_sys的状态置为STOP状态*/
			purge_sys->state = TRX_STOP_PURGE;
			trx_purge_truncate_if_arr_empty();

			if(srv_print_thread_releases)
				printf("Purge: No logs left in the history list; pages handled %lu\n", purge_sys->n_pages_handled);

			mutex_exit(&(purge_sys->mutex));

			return NULL;
		}
	}

	/*当前完成purge的page数量大于limit，无需继续*/
	if(purge_sys->n_pages_handled >= purge_sys->handle_limit) {
		purge_sys->state = TRX_STOP_PURGE;
		trx_purge_truncate_if_arr_empty();
		mutex_exit(&(purge_sys->mutex));

		return(NULL);
	}	

	/*trx no到了规定purge的trx_limit上限*/
	if(ut_dulint_cmp(purge_sys->purge_trx_no, purge_sys->view->low_limit_no) >= 0){
		purge_sys->state = TRX_STOP_PURGE;
		trx_purge_truncate_if_arr_empty();
		mutex_exit(&(purge_sys->mutex));
		return NULL;
	}

	/*构建一个roll ptr*/
	*roll_ptr = trx_undo_build_roll_ptr(FALSE, purge_sys->rseg->id, purge_sys->page_no, purge_sys->offset);
	/*在purge设置一个cell状态信息对象*/
	*cell = trx_purge_arr_store_info(purge_sys->purge_trx_no, purge_sys->purge_undo_no);

	ut_ad(ut_dulint_cmp(purge_sys->purge_trx_no, (purge_sys->view)->low_limit_no) < 0);
	undo_rec = trx_purge_get_next_rec(heap);

	mutex_exit(&(purge_sys->mutex));

	return undo_rec;
}

/*cell对应的purge操作已经完成，释放掉占用的cell槽位*/
void trx_purge_rec_release(trx_undo_inf_t* cell)
{
	trx_undo_arr_t*	arr;

	mutex_enter(&(purge_sys->mutex));
	arr = purge_sys->arr;
	trx_purge_arr_remove_info(cell);
	mutex_exit(&(purge_sys->mutex));
}

/*批量运行purage操作*/
ulint trx_purge()
{
	que_thr_t*	thr;
	ulint		old_pages_handled;

	mutex_enter(&(purge_sys->mutex));
	if(purge_sys->trx->n_active_thrs > 0){
		mutex_exit(&(purge_sys->mutex));
		ut_a(0);
		return 0;
	}

	rw_lock_x_lock(&(purge_sys->latch));

	mutex_enter(kernel_mutex);

	/*关闭过去的pruge view*/
	read_view_close(purge_sys->view);
	purge_sys->view = NULL;
	mem_heap_empty(purge_sys->heap);

	purge_sys->view = read_view_oldest_copy_or_open_new(NULL, purge_sys->heap);
	mutex_exit(&kernel_mutex);	

	rw_lock_x_unlock(&(purge_sys->latch));
	purge_sys->state = TRX_PURGE_ON;
	/*每次purge 20个undo log page*/
	purge_sys->handle_limit = purge_sys->n_pages_handled + 20;
	old_pages_handled = purge_sys->n_pages_handled;

	mutex_exit(&(purge_sys->mutex));
	mutex_enter(&kernel_mutex);
	/*启动purge命令*/
	thr = que_fork_start_command(purge_sys->query, SESS_COMM_EXECUTE, 0);
	mutex_exit(&kernel_mutex);

	if(srv_print_thread_releases)
		printf("Starting purge\n");

	que_run_threads(thr);

	/*答应开始purge的page个数*/
	if(srv_print_thread_releases)
		printf("Purge ends; pages handled %lu\n", purge_sys->n_pages_handled);

	/*打印完成的purge的个数*/
	return purge_sys->n_pages_handled - old_pages_handled;
}

void trx_purge_sys_print()
{
	fprintf(stderr, "InnoDB: Purge system view:\n");
	read_view_print(purge_sys->view);

	fprintf(stderr, "InnoDB: Purge trx n:o %lu %lu, undo n_o %lu %lu\n",
		ut_dulint_get_high(purge_sys->purge_trx_no),
		ut_dulint_get_low(purge_sys->purge_trx_no),
		ut_dulint_get_high(purge_sys->purge_undo_no),
		ut_dulint_get_low(purge_sys->purge_undo_no));

	fprintf(stderr,
		"InnoDB: Purge next stored %lu, page_no %lu, offset %lu,\n"
		"InnoDB: Purge hdr_page_no %lu, hdr_offset %lu\n",
		purge_sys->next_stored,
		purge_sys->page_no,
		purge_sys->offset,
		purge_sys->hdr_page_no,
		purge_sys->hdr_offset);
}


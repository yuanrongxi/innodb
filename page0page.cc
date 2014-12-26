#include "page0page.h"
#include "page0cur.h"
#include "lock0lock.h"
#include "fut0lst.h"
#include "btr0sea.h"
#include "buf0buf.h"

page_t* page_template = NULL;

//////////////////////////////////////////////////////////////////////////////////
/*查找一个owned不为0的rec所在的槽位*/
ulint page_dir_find_owner_slot(rec_t* rec)
{
	ulint			i;
	ulint			steps = 0;
	page_t*			page;	
	page_dir_slot_t*	slot;
	rec_t*			original_rec = rec;
	char			err_buf[1000];

	ut_ad(page_rec_check(rec));
	while(rec_get_n_owned(rec) == 0){
		steps ++;
		rec = page_rec_get_next(rec);
	}

	page = buf_frame_align(rec);

	i = page_dir_get_n_slots(page) - 1;
	slot = page_dir_get_nth_slot(page, i);

	while(page_dir_slot_get_rec(slot) != rec){
		if(i == 0){
			fprintf(stderr, "InnoDB: Probable data corruption on page %lu\n", buf_frame_get_page_no(page));
			rec_sprintf(err_buf, 900, original_rec);

			fprintf(stderr, "InnoDB: Original record %s\n" "InnoDB: on that page. Steps %lu.\n", err_buf, steps);
			rec_sprintf(err_buf, 900, rec);

			fprintf(stderr,"InnoDB: Cannot find the dir slot for record %s\n"
				"InnoDB: on that page!\n", err_buf);
			buf_page_print(page);

			ut_a(0);
		}

		i --;
		slot = page_dir_get_nth_slot(page, i);
	}

	return i;
}

/*检查slot在page directory中的信息一致性,感觉只在DEBUG有效*/
static ibool page_dir_slot_check(page_dir_slot_t* slot)
{
	page_t*	page;
	ulint	n_slots;
	ulint	n_owned;

	ut_a(slot);

	page = buf_frame_align(slot);

	n_slots = page_header_get_field(page, PAGE_N_DIR_SLOTS);

	ut_a(slot <= page_dir_get_nth_slot(page, 0));
	ut_a(slot >= page_dir_get_nth_slot(page, n_slots - 1));

	ut_a(page_rec_check(page + mach_read_from_2(slot)));

	n_owned = rec_get_n_owned(page + mach_read_from_2(slot));
	if(slot == page_dir_get_nth_slot(page, 0))/*infimum对应的slot的记录owned = 1*/
		ut_a(n_owned == 1);
	else if(slot == page_dir_get_nth_slot(page, n_slots - 1)){ /*supremum对应slot的记录owned = [1, 8]*/
		ut_a(n_owned >= 1);
		ut_a(n_owned <= PAGE_DIR_SLOT_MAX_N_OWNED);
	}
	else{ /*其他槽位 [4, 8]*/
		ut_a(n_owned >= PAGE_DIR_SLOT_MIN_N_OWNED);
		ut_a(n_owned <= PAGE_DIR_SLOT_MAX_N_OWNED);
	}

	return TRUE;
}

void page_set_max_trx_id(page_t* page, dunlint trx_id)
{
	buf_block_t* block;

	ut_ad(page);

	block = buf_block_agign(page);

	/*获得b tree的seach latch权限*/
	if(block->is_hashed)
		rw_lock_x_lock(&btr_search_latch);
	/*设置事务ID*/
	mach_write_to_8(page + PAGE_HEADER + PAGE_MAX_TRX_ID, trx_id);

	if(block->is_hashed)
		rw_lock_x_unlock(&btr_search_latch);
}

byte* page_mem_alloc(page_t* page, ulint need, ulint* heap_no)
{
	rec_t*	rec;
	byte*	block;
	ulint	avl_space;
	ulint	garbage;

	ut_ad(page && heap_no);

	/*获得free第一个记录的指针*/
	rec = page_header_get_ptr(page, PAGE_FREE);
	if(rec != NULL && rec_get_size(rec) >= need){
		/*将FREE指向后一个rec*/
		page_header_set_ptr(page, PAGE_FREE, page_rec_get_next(rec));

		/*修改PAGE_GARBAGE,已经删除rec的空间大小*/
		garbage = page_header_get_field(page, PAGE_GARBAGE);
		ut_ad(garbage >= need);

		page_header_set_field(page, PAGE_GARBAGE, garbage - need);
		*heap_no = rec_get_heap_no(rec);

		return rec_get_start(rec);
	}

	/*无法在删除的记录列表中找到合适的空间，直接在heap上找*/
	avl_space = page_get_max_insert_size(page, 1);
	if(avl_space >= need){
		block = page_header_get_ptr(page, PAGE_HEAP_TOP);
		/*修改page heap top*/
		page_header_set_ptr(page, PAGE_HEAP_TOP, block + need);
		*heap_no = page_header_get_field(page, PAGE_N_HEAP);
		page_header_set_field(page, PAGE_N_HEAP, *heap_no + 1);

		return block;
	}

	return NULL;
}

/*写一条page建立的redo log*/
UNIV_INLINE void page_create_write_log(buf_frame_t* frame, mtr_t* mtr)
{
	mlog_write_initial_log_record(frame, MLOG_PAGE_CREATE, mtr);
}

byte* page_parse_create(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	ut_ad(ptr && end_ptr);

	if(page != NULL)
		page_create(page, mtr);

	return ptr;
}

page_t* page_create(buf_frame_t* frame, mtr_t* mtr)
{
	page_dir_slot_t* slot;
	mem_heap_t*	heap;
	dtuple_t*	tuple;	
	dfield_t*	field;
	byte*		heap_top;
	rec_t*		infimum_rec;
	rec_t*		supremum_rec;
	page_t*		page;

	ut_ad(frame && mtr);
	ut_ad(PAGE_BTR_IBUF_FREE_LIST + FLST_BASE_NODE_SIZE <= PAGE_DATA);
	ut_ad(PAGE_BTR_IBUF_FREE_LIST_NODE + FLST_NODE_SIZE <= PAGE_DATA);

	buf_frame_modify_clock_inc(frame);

	/*记录一条index page创建的日志*/
	page_create_write_log(frame, mtr);

	page = frame;
	/*设置page类型*/
	fil_page_set_type(page, FIL_PAGE_INDEX);
	/*用模板中的信息进行初始化，这样速度更快*/
	if (page_template) {
		ut_memcpy(page + PAGE_HEADER, page_template + PAGE_HEADER, PAGE_HEADER_PRIV_END);
		ut_memcpy(page + PAGE_DATA, page_template + PAGE_DATA, PAGE_SUPREMUM_END - PAGE_DATA);
		ut_memcpy(page + UNIV_PAGE_SIZE - PAGE_EMPTY_DIR_START, page_template + UNIV_PAGE_SIZE - PAGE_EMPTY_DIR_START, PAGE_EMPTY_DIR_START - PAGE_DIR);
		return(frame);
	}

	heap = mem_heap_create(200);

	/*	create infimum record*/
	tuple = dtuple_create(heap, 1);
	field = dtuple_get_nth_field(tuple, 0);
	dfield_set_data(field, "infimum", strlen("infimum") + 1);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 20, 0);

	heap_top = page + PAGE_DATA;

	infimum_rec = rec_convert_dtuple_to_rec(heap_top, tuple);
	ut_ad(infimum_rec == page + PAGE_INFIMUM);
	rec_set_n_owned(infimum_rec, 1);
	rec_set_heap_no(infimum_rec, 0);

	heap_top = rec_get_end(infimum_rec);

	/*create supremum rec*/
	tuple = dtuple_create(heap, 1);
	field = dtuple_get_nth_field(tuple, 0);

	dfield_set_data(field, "supremum", strlen("supremum") + 1);
	dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH, 20, 0);

	supremum_rec = rec_convert_dtuple_to_rec(heap_top, tuple);

	ut_ad(supremum_rec == page + PAGE_SUPREMUM);

	rec_set_n_owned(supremum_rec, 1);
	rec_set_heap_no(supremum_rec, 1);

	heap_top = rec_get_end(supremum_rec);

	ut_ad(heap_top == page + PAGE_SUPREMUM_END);

	mem_heap_free(heap);

	/*初始化page header*/
	page_header_set_field(page, PAGE_N_DIR_SLOTS, 2);
	page_header_set_ptr(page, PAGE_HEAP_TOP, heap_top);
	page_header_set_field(page, PAGE_N_HEAP, 2);
	page_header_set_ptr(page, PAGE_FREE, NULL);
	page_header_set_field(page, PAGE_GARBAGE, 0);
	page_header_set_ptr(page, PAGE_LAST_INSERT, NULL);
	page_header_set_field(page, PAGE_N_RECS, 0);
	page_set_max_trx_id(page, ut_dulint_zero);

	/*设置第一个slot*/
	slot = page_dir_get_nth_slot(page, 0);
	page_dir_slot_set_rec(slot, infimum_rec);
	/*设置最后一个slot*/
	slot = page_dir_get_nth_slot(page, 1);
	page_dir_slot_set_rec(slot, supremum_rec);

	/*设置记录关联*/
	rec_set_next_offs(infimum_rec, (ulint)(supremum_rec - page)); 
	rec_set_next_offs(supremum_rec, 0);

	return page;
}

void page_copy_rec_list_end_no_locks(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr)
{
	page_cur_t	cur1;
	page_cur_t	cur2;
	rec_t*		sup;
	/*获得page的游标,游标并且指向rec*/
	page_cur_position(rec, &cur1);
	if(page_cur_is_before_first(&cur1))
		page_cur_move_to_next(&cur1);

	/*设置new_page的游标,从infimum开始*/
	page_cur_set_before_first(new_page, &cur2);

	/*进行记录copy*/
	sup = page_get_supremum_rec(page);
	while(sup != page_cur_get_rec(&cur1)){
		ut_a(page_cur_rec_insert(&cur2, page_cur_get_rec(&cur1), mtr));
		page_cur_move_to_next(&cur1);
		page_cur_move_to_next(&cur2);
	}
}

/*将page中的rec之后的记录全部复制到new page,包括rec*/
void page_copy_rec_list_end(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr)
{
	if(page_header_get_field(new_page, PAGE_N_HEAP) == 2)
		page_copy_rec_list_end_to_created_page(new_page, page, rec, mtr);
	else
		page_copy_rec_list_end_no_locks(new_page, page, rec, mtr);

	lock_move_rec_list_end(new_page, page, rec);
	page_update_max_trx_id(new_page, page_get_max_trx_id(page));
	btr_search_move_or_delete_hash_entries(new_page, page);
}

/*将page中在rec之前的记录全部拷贝到new page当中,不包括rec*/
void page_copy_rec_list_start(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr)
{
	page_cur_t	cur1;
	page_cur_t	cur2;
	rec_t*	    old_end;

	page_cur_set_before_first(&cur1);
	if(rec == page_cur_get_rec(&cur1))
		return ;

	page_cur_move_to_next(&cur1);
	page_cur_set_after_last(new_page, &cur);
	page_cur_move_to_prev(&cur2);
	old_end = page_cur_get_rec(&cur2);

	while(page_cur_get_rec(&cur1) != rec){
		ut_a(page_cur_rec_insert(&cur2, page_cur_get_rec(&cur1), mtr));

		page_cur_move_to_next(&cur1);
		page_cur_move_to_next(&cur2);
	}

	/*设置max trx id*/
	lock_move_rec_list_start(new_page, page, rec, old_end);
	page_update_max_trx_id(new_page, page_get_max_trx_id(page));
	btr_search_move_or_delete_hash_entries(new_page, page);	
}

/*删除记录的mtr log*/
UNIV_INLINE void page_delete_rec_list_write_log(page_t* page, rec_t* rec, byte type, mtr_t* mtr)
{
	ut_ad(type == MLOG_LIST_END_DELETE || type == MLOG_LIST_START_DELETE);

	mlog_write_initial_log_record(page, type, mtr);
	/*将rec的页内偏移写入到mtr log中*/
	mlog_catenate_ulint(mtr, rec - page, MLOG_2BYTES);
}

byte* page_parse_delete_rec_list(byte* type, byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	ulint offset;

	ut_ad((type == MLOG_LIST_END_DELETE) || (type == MLOG_LIST_START_DELETE));

	if(end_ptr < ptr + 2)
		return NULL;

	offset = mach_read_from_2(ptr);
	ptr += 2;

	if(!page)
		return ptr;

	if(type == MLOG_LIST_END_DELETE)
		page_delete_rec_list_end(page, page + offset, ULINT_UNDEFINED, ULINT_UNDEFINED, mtr);
	else
		page_delete_rec_list_start(page, page + offset, mtr);

	return ptr;
}

void page_delete_rec_list_end(page_t* page, rec_t* rec, ulint n_recs, ulint size, mtr_t* mtr)
{
	page_dir_slot_t* slot;
	ulint	slot_index;
	rec_t*	last_rec;
	rec_t*	prev_rec;
	rec_t*	free;
	rec_t*	rec2;
	ulint	count;
	ulint	n_owned;
	rec_t*	sup;

	page_header_set_ptr(page, PAGE_LAST_INSERT, NULL);

	buf_frame_modify_clock_inc(page);

	sup = page_get_supremum_rec(page);

	if(rec == page_get_infimum_rec(page))
		rec = page_rec_get_next(rec);

	page_delete_rec_list_write_log(page, rec, MLOG_LIST_END_DELETE, mtr);
	if(rec == sup)
		return ;

	prev_rec = page_rec_get_prev(rec);
	last_rec = page_rec_get_prev(sup);

	if(size == ULINT_UNDEFINED  || n_recs == ULINT_UNDEFINED){
		size = 0;
		n_recs = 0;
		rec2 = rec;
		/*计算要删除的记录数和空间大小*/
		while(rec2 != sup){
			size += rec_get_size(rec2);
			n_recs++;
			rec2 = page_rec_get_next(rec2);
		}
	}

	rec2 = rec;
	count = 0;

	/*定位到rec2所在slot的owned存储值的rec,将剩余未删除的rec 个数作为最后一个slot的值*/
	while(rec_get_n_owned(rec2) == 0){
		count ++;
		rec2 = page_rec_get_next(rec2);
	}

	ut_ad(rec_get_n_owned(rec2) - count > 0);

	/*更改槽位对应记录的owned*/
	n_owned = rec_get_n_owned(rec2) - count;
	slot_index = page_dir_find_owner_slot(rec2);
	slot = page_dir_get_nth_slot(page, slot_index);
	page_dir_slot_set_rec(slot, sup);
	page_dir_slot_set_n_owned(slot, n_owned);

	/*重新设置使用的个数*/
	page_header_set_field(page, PAGE_N_DIR_SLOTS, slot_index + 1);

	page_rec_set_next(prev_rec, page_get_supremum_rec(page));

	free = page_header_get_ptr(page, PAGE_FREE);
	/*设置LAST REC的后一个记录指针是free rec*/
	page_rec_set_next(last_rec, free);
	/*将rec作为free的头指针*/
	page_header_set_ptr(page, PAGE_FREE, rec);

	/*重新设置释放的rec的可用空间字节数和已经使用的记录数*/
	page_header_set_field(page, PAGE_GARBAGE, size + page_header_get_field(page, PAGE_GARBAGE));
	page_header_set_field(page, PAGE_N_RECS, (ulint)(page_get_n_recs(page) - n_recs));
}

/*删除page中rec之前的所有记录,不包括rec*/
void page_delete_rec_list_start(page_t* page, rec_t* rec, mtr_t* mtr)
{
	page_cur_t	cur1;
	ulint		log_mode;

	page_delete_rec_list_write_log(page, rec, MLOG_LIST_START_DELETE, mtr);

	page_cur_set_before_first(page, &cur1);
	if(rec == page_cur_get_rec(&cur1))
		return ;

	page_cur_move_to_next(&cur1);
	log_mode = mtr_set_log_mode(mtr, MTR_LOG_NONE); /*如果前面没有记录删除，设置一个空操作日志*/
	while(page_cur_get_rec(&cur1) != rec){
		page_cur_delete_rec(cur1);
	}
	/*重新设置删除日志类型*/
	mtr_set_log_mode(mtr, log_mode);
}

/*将page中rec之后的记录全部move到new page中，包括rec*/
void page_move_rec_list_end(page_t* new_page, page_t* page, rec_t* split_rec, mtr_t* mtr)
{
	ulint	old_data_size;
	ulint	new_data_size;
	ulint	old_n_recs;
	ulint	new_n_recs;

	old_data_size = page_get_data_size(new_page);
	old_n_recs = page_get_n_recs(page);
	/*将rec之后的记录拷贝至new page当中*/
	page_copy_rec_list_end(new_page, page, split_rec, mtr);

	new_data_size = page_get_data_size(new_page);
	new_n_recs = page_get_n_recs(new_page);

	/*从page中删除被转移的rec*/
	page_delete_rec_list_end(page, split_rec, new_n_recs - old_n_recs,
		new_data_size - old_data_size, mtr);
}

void page_move_rec_list_start(page_t* new_page, page_t* page, split_rec, mtr_t* mtr)
{
	page_copy_rec_list_start(new_page, page, split_rec, mtr);
	page_delete_rec_list_start(pace, split_rec, mtr);
}

/*更新对应rec的page no*/
void page_rec_write_index_page_no(rec_t* rec, ulint i, ulint page_no, mtr_t* mtr)
{
	byte* data;
	ulint len;

	data = rec_get_nth_field(rec, i, &len);

	ut_ad(len == 4);

	mlog_write_ulint(data, page_no, MLOG_4BYTES, mtr);
}

UNIV_INLINE void page_dir_delete_slots(page_t* page, ulint start, ulint n)
{
	page_dir_slot_t*	slot;
	ulint			i;
	ulint			sum_owned = 0;
	ulint			n_slots;
	rec_t*			rec;

	ut_ad(n == 1);	
	ut_ad(start > 0);
	ut_ad(start + n < page_dir_get_n_slots(page));

	n_slots = page_dir_get_n_slots(page);

	/*将从start开始后的N个slot全部清空*/
	for(i = start; i < start + n; i ++){
		slot = page_dir_get_nth_slot(page, i);
		sum_owned += page_dir_slot_get_n_owned(slot);
		page_dir_slot_set_n_owned(slot, 0);
	}

	slot = page_dir_get_nth_slot(page, start + n);
	page_dir_slot_set_n_owned(slot, sum_owned + page_dir_slot_get_n_owned(slot));

	/*向前移动slot信息*/
	for(i = start + n; i < n_slots; i ++){
		slot = page_dir_get_nth_slot(page, i);
		rec = page_dir_slot_get_rec(slot);

		slot = page_dir_get_nth_slot(page, i - n);
		page_dir_slot_set_rec(slot, rec);
	}

	page_header_set_field(page, PAGE_N_DIR_SLOTS, n_slots - n);
}

UNIV_INLINE void page_dir_add_slots(page_t* page, ulint start, ulint n)
{
	page_dir_slot_t*	slot;
	ulint			n_slots;
	ulint			i;
	rec_t*			rec;

	ut_ad(n == 1);

	n_slots = page_dir_get_n_slots(page);

	ut_ad(start < n_slots -1);

	page_header_set_field(page, PAGE_N_DIR_SLOTS, n_slots + n);

	for(i = n_slots - 1; i > start; i --){
		slot = page_dir_get_nth_slot(page, i);
		rec = page_dir_slot_get_rec(slot);

		slot = page_dir_get_nth_slot(page, i + n);
		page_dir_slot_set_rec(slot, rec);
	}
}

/*将一个slot分裂成2个*/
void page_dir_split_slot(page_t* page, ulint slot_no)
{
	rec_t*			rec;
	page_dir_slot_t*	new_slot;
	page_dir_slot_t*	prev_slot;
	page_dir_slot_t*	slot;
	ulint			i;
	ulint			n_owned;

	ut_ad(page);
	ut_ad(slot_no > 0);

	slot = page_dir_get_nth_slot(page, slot_no);
	n_owned = page_dir_slot_get_n_owned(slot);
	ut_ad(n_owned == PAGE_DIR_SLOT_MAX_N_OWNED);

	prev_slot = page_dir_get_nth_slot(page, slot_no - 1);
	rec = page_dir_slot_get_rec(prev_slot);

	/*查找需要设置owned的rec*/
	for(i = 0; i < n_owned /2; i ++)
		rec = page_rec_get_next(rec);

	ut_ad(n_owned / 2 >= PAGE_DIR_SLOT_MIN_N_OWNED);

	/*向后移动一个slot*/
	page_dir_add_slots(page, slot_no - 1, 1);
	/*在空出来的slot进行分裂操作*/
	new_slot = page_dir_get_nth_slot(page, slot_no);
	slot = page_dir_get_nth_slot(page, slot_no + 1);

	page_dir_slot_set_n_owned(slot, n_owned - (n_owned / 2));
}

void page_dir_balance_slot(page_t* page, ulint slot_no)
{
	page_dir_slot_t*	slot;
	page_dir_slot_t*	up_slot;
	ulint			n_owned;
	ulint			up_n_owned;
	rec_t*			old_rec;
	rec_t*			new_rec;

	ut_ad(page);
	ut_ad(slot_no > 0);

	slot = page_dir_get_nth_slot(page, slot_no);
	if(slot_no == page_dir_get_n_slots(page) - 1) /*是最后一个*/
		return ;

	up_slot = page_dir_get_nth_slot(page, slot_no + 1);
	n_owned = page_dir_slot_get_n_owned(slot);
	up_n_owned = page_dir_slot_get_n_owned(up_slot);

	ut_ad(n_owned == PAGE_DIR_SLOT_MIN_N_OWNED - 1);
	ut_ad(2 * PAGE_DIR_SLOT_MIN_N_OWNED - 1 <= PAGE_DIR_SLOT_MAX_N_OWNED);

	if(up_n_owned > PAGE_DIR_SLOT_MIN_N_OWNED){
		/*向后移一格rec，并设置owned*/
		old_rec = page_dir_slot_get_rec(slot);
		new_rec = page_rec_get_next(old_rec);

		rec_set_n_owned(old_rec, 0);
		rec_set_n_owned(new_rec, n_owned + 1);

		page_dir_slot_set_rec(slot, new_rec);
		page_dir_slot_set_n_owned(up_slot, up_n_owned - 1);
	}
	else
		page_dir_delete_slots(page, slot_no, 1);
}

/*找到记录集的中间记录rec指针地址*/
rec_t* page_get_middle_rec(page_t* page)
{
	page_dir_slot_t*	slot;
	ulint			middle;
	ulint			i;
	ulint			n_owned;
	ulint			count;
	rec_t*			rec;

	middle = (page_get_n_recs(page) + 2) / 2;
	count = 0;

	/*找到一半记录数的slot位置*/
	for(i = 0;; i ++){
		slot = page_dir_get_nth_slot(page, i);
		n_owned = page_dir_slot_get_n_owned(slot);

		if(count + n_owned > middle)
			break;
		else
			count += n_owned;
	}

	ut_ad(i > 0);

	slot = page_dir_get_nth_slot(page, i - 1);
	rec = page_dir_slot_get_rec(slot);
	rec = page_rec_get_next(rec);

	/*在slot中精确定位*/
	for (i = 0; i < middle - count; i++) {
		rec = page_rec_get_next(rec);
	}

	return rec;
}

/*在rec之前有多少天记录*/
ulint page_rec_get_n_recs_before(rec_t* rec)
{
	page_dir_slot_t*	slot;
	rec_t*			slot_rec;
	page_t*			page;
	ulint			i;
	lint			n	= 0;

	ut_ad(page_rec_check(rec));

	page = buf_frame_align(rec);
	/*找到slot首位的rec*/
	while(rec_get_n_owned(rec) == 0){
		rec = page_rec_get_next(rec);
		n --;
	}

	/*进行slot扫描计算统计*/
	for(i = 0; ; i++){
		slot = page_dir_get_nth_slot(page, i);
		slot_rec = page_dir_slot_get_rec(slot);

		n += rec_get_n_owned(slot_rec);

		if (rec == slot_rec)
			break;
	}

	n --;

	return n;
}

void page_rec_print(rec_t* rec)
{
	rec_print(rec);

	printf(     		"            n_owned: %lu; heap_no: %lu; next rec: %lu\n",
		rec_get_n_owned(rec),
		rec_get_heap_no(rec),
		rec_get_next_offs(rec));

	page_rec_check(rec);
	rec_validate(rec);
}


void page_dir_print(page_t* page, ulint pr_n)
{
	ulint			n;
	ulint			i;
	page_dir_slot_t*	slot;

	n = page_dir_get_n_slots(page);

	printf("--------------------------------\n");
	printf("PAGE DIRECTORY\n");
	printf("Page address %lx\n", (ulint)page);
	printf("Directory stack top at offs: %lu; number of slots: %lu\n",  (ulint)(page_dir_get_nth_slot(page, n - 1) - page), n);

	for(i = 0; i < n; i ++){
		slot = page_dir_get_nth_slot(page, i);
		if(i == pr_n && i < n - pr_n){
			printf("   ...    ");
		}

		if(i < pr_n || i >= n - pr_n)
			printf("Contents of slot: %lu: n_owned: %lu, rec offs: %lu\n", i, page_dir_slot_get_n_owned(slot),
			(ulint)(page_dir_slot_get_rec(slot) - page));
	}
	printf("Total of %lu records \n", 2 + page_get_n_recs(page));
	printf("----------------------------------\n");
}

void page_print_list(page_t* page, ulint pr_n)
{
	page_cur_t	cur;
	rec_t*		rec;
	ulint		count;
	ulint		n_recs;

	printf("--------------------------------\n");
	printf("PAGE RECORD LIST\n");
	printf("Page address %lu\n", (ulint)page);

	n_recs = page_get_n_recs(page);
	/*将游标设置在起始infimum记录上*/
	page_cur_set_before_first(page, &cur);
	cout = 0;
	for(;;){
		rec = (&cur)->rec;
		page_rec_print(rec);

		if(count == pr_n)
			break;

		if(page_cur_is_after_last(&cur))
			break;

		page_cur_move_to_next(&cur);
		cout ++;
	}

	if(n_recs > 2 * pr_n)
		printf("  ...   \n");

	for(;;){
		if (page_cur_is_after_last(&cur)) {
			break;
		}

		page_cur_move_to_next(&cur);
		if(count + pr_n >= n_recs){
			rec = (&cur)->rec;
			page_rec_print(rec);
		}
		count ++;
	}

	printf("Total of %lu records \n", count + 1);	
	printf("--------------------------------\n");
}

void page_header_print(page_t* page)
{
	printf("--------------------------------\n");
	printf("PAGE HEADER INFO\n");
	printf("Page address %lx, n records %lu\n", (ulint)page,
		page_header_get_field(page, PAGE_N_RECS));				/*存在的记录数，不包括infimum supremum*/

	printf("n dir slots %lu, heap top %lu\n",
		page_header_get_field(page, PAGE_N_DIR_SLOTS),			/*slots数和page_heap的空闲位置*/
		page_header_get_field(page, PAGE_HEAP_TOP));			

	printf("Page n heap %lu, free %lu, garbage %lu\n",
		page_header_get_field(page, PAGE_N_HEAP),				/*heap中已经分配的记录数量，PAGE_N_RECS + FREE RECS + 2,*/
		page_header_get_field(page, PAGE_FREE),					/*删除回收的记录列表起始位置*/
		page_header_get_field(page, PAGE_GARBAGE));				/*删除回收的空间大小*/

	printf("Page last insert %lu, direction %lu, n direction %lu\n",
		page_header_get_field(page, PAGE_LAST_INSERT),
		page_header_get_field(page, PAGE_DIRECTION),
		page_header_get_field(page, PAGE_N_DIRECTION));
}

void page_print(page_t* page, ulint dn, ulint rn)
{
	page_header_print(page);
	page_dir_print(page, dn);
	page_print_list(page, rn);
}

/*rec的合法性校验*/
ibool page_rec_validate(rec_t* rec)
{
	ulint	n_owned;
	ulint	heap_no;
	page_t* page;

	page = buf_frame_align(rec);

	page_rec_check(rec);
	rec_validate(rec);

	n_owned = rec_get_n_owned(rec);
	heap_no = rec_get_heap_no(rec);

	if(!(n_owned <= PAGE_DIR_SLOT_MAX_N_OWNED)){
		fprintf(stderr, "Dir slot n owned too big %lu\n", n_owned);
		return(FALSE);
	}

	if(!(heap_no < page_header_get_field(page, PAGE_N_HEAP))){
		fprintf(stderr, "Heap no too big %lu %lu\n", heap_no, page_header_get_field(page, PAGE_N_HEAP));
		return(FALSE);
	}

	return TRUE;
}

ibool page_validate(page_t* page, dict_index_t* index)
{
	page_dir_slot_t* slot;
	mem_heap_t*	heap;
	page_cur_t 	cur;
	byte*		buf;
	ulint		count;
	ulint		own_count;
	ulint		slot_no;
	ulint		data_size;
	rec_t*		rec;
	rec_t*		old_rec	= NULL;
	ulint		offs;
	ulint		n_slots;
	ibool		ret	= FALSE;
	ulint		i;
	char           	err_buf[1000];

	heap = mem_heap_create(UNIV_PAGE_SIZE);

	buf = mem_heap_alloc(heap, UNIV_PAGE_SIZE);
	for(i = 0; i < UNIV_PAGE_SIZE; i ++)
		buf[i] = 0;

	n_slots = page_dir_get_n_slots(page);
	if(!(page_header_get_ptr(page, PAGE_HEAP_TOP) <= page_dir_get_nth_slot(page, n_slots - 1))){
		fprintf(stderr, "Record heap and dir overlap on a page in index %s, %lu, %lu\n",
			index->name, (ulint)page_header_get_ptr(page, PAGE_HEAP_TOP),
			(ulint)page_dir_get_nth_slot(page, n_slots - 1));

		goto func_exit;
	}

	count = 0;
	data_size = 0;
	own_count = 1;
	slot_no = 0;
	slot = page_dir_get_nth_slot(page, slot_no);

	page_cur_set_before_first(page, &cur);
	for(;;){
		rec = (&cur)->rec;
		if(!page_rec_validate(rec))
			goto func_exit;

		if ((count >= 2) && (!page_cur_is_after_last(&cur))) {
			if (!(1 == cmp_rec_rec(rec, old_rec, index))) {
				fprintf(stderr, "Records in wrong order in index %s\n", index->name);
				rec_sprintf(err_buf, 900, old_rec);
				fprintf(stderr, "InnoDB: record %s\n", err_buf);

				rec_sprintf(err_buf, 900, rec);
				fprintf(stderr, "InnoDB: record %s\n", err_buf);

				goto func_exit;
			}
		}

		if ((rec != page_get_supremum_rec(page)) && (rec != page_get_infimum_rec(page)))
			data_size += rec_get_size(rec);

		offs = rec_get_start(rec) - page;

		for(i = 0; i < rec_get_size(rec); i ++){
			if(!buf[offs + 1] == 0){
				fprintf(stderr, "Record overlaps another in index %s \n", index->name);
				goto func_exit;
			}
			buf[offs + i] = 1;
		}

		if (rec_get_n_owned(rec) != 0) {
			/* This is a record pointed to by a dir slot */
			if (rec_get_n_owned(rec) != own_count) {
				fprintf(stderr,
					"Wrong owned count %lu, %lu, in index %s\n",
					rec_get_n_owned(rec), own_count,
					index->name);

				goto func_exit;
			}

			if (page_dir_slot_get_rec(slot) != rec) {
				fprintf(stderr,
					"Dir slot does not point to right rec in %s\n",
					index->name);

				goto func_exit;
			}

			page_dir_slot_check(slot);

			own_count = 0;
			if (!page_cur_is_after_last(&cur)) {
				slot_no++;
				slot = page_dir_get_nth_slot(page, slot_no);
			}
		}

		if (page_cur_is_after_last(&cur)) {
			break;
		}

		if (rec_get_next_offs(rec) < FIL_PAGE_DATA
			|| rec_get_next_offs(rec) >= UNIV_PAGE_SIZE) {
				fprintf(stderr,
					"Next record offset wrong %lu in index %s\n",
					rec_get_next_offs(rec), index->name);

				goto func_exit;
		}

		count++;		
		page_cur_move_to_next(&cur);
		own_count++;
		old_rec = rec;
	}

	if (rec_get_n_owned(rec) == 0) {
		fprintf(stderr, "n owned is zero in index %s\n", index->name);
		goto func_exit;
	}

	if (slot_no != n_slots - 1) {
		fprintf(stderr, "n slots wrong %lu %lu in index %s\n",
			slot_no, n_slots - 1, index->name);
		goto func_exit;
	}		

	if (page_header_get_field(page, PAGE_N_RECS) + 2 != count + 1) {
		fprintf(stderr, "n recs wrong %lu %lu in index %s\n", page_header_get_field(page, PAGE_N_RECS) + 2,  count + 1, index->name);

		goto func_exit;
	}

	if (data_size != page_get_data_size(page)) {
		fprintf(stderr, "Summed data size %lu, returned by func %lu\n", data_size, page_get_data_size(page));
		goto func_exit;
	}

	/* Check then the free list */
	rec = page_header_get_ptr(page, PAGE_FREE);

	while (rec != NULL) {
		if (!page_rec_validate(rec))
			goto func_exit;

		count++;	
		offs = rec_get_start(rec) - page;

		for (i = 0; i < rec_get_size(rec); i++){
			if (buf[offs + i] != 0) {
				fprintf(stderr, "Record overlaps another in free list, index %s \n", index->name);
				goto func_exit;
			}

			buf[offs + i] = 1;
		}

		rec = page_rec_get_next(rec);
	}

	if (page_header_get_field(page, PAGE_N_HEAP) != count + 1)
		fprintf(stderr, "N heap is wrong %lu %lu in index %s\n", page_header_get_field(page, PAGE_N_HEAP), count + 1, index->name);

	ret = TRUE;	

func_exit:
	mem_heap_free(heap);

	return(ret);
}

/*通过heap no偏移找到对应的rec*/
rec_t* page_find_rec_with_heap_no(page_t* page, ulint heap_no)
{
	page_cur_t	cur;
	rec_t*		rec;

	page_cur_set_before_first(page, &cur);

	for (;;) {
		rec = (&cur)->rec;
		if (rec_get_heap_no(rec) == heap_no)
			return(rec);

		if (page_cur_is_after_last(&cur))
			return(NULL);

		page_cur_move_to_next(&cur);
	}
}





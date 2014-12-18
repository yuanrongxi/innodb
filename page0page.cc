#include "page0page.h"
#include "lock0lock.h"
#include "fut0lst.h"
#include "btr0sea.h"
#include "buf0buf.h"

page_t* page_template = NULL;

//////////////////////////////////////////////////////////////////////////////////

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
	if(slot == page_dir_get_nth_slot(page, 0))
		ut_a(n_owned == 1);
	else if(slot == page_dir_get_nth_slot(page, n_slots - 1))
		ut_a(n_owned >= 1);
		ut_a(n_owned <= PAGE_DIR_SLOT_MAX_N_OWNED);
	else{
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

	/*获得空闲记录的指针*/
	rec = page_header_get_ptr(page, PAGE_FREE);
	if(rec != NULL && rec_get_size(rec) >= need){
		page_header_set_ptr(page, PAGE_FREE, page_rec_get_next(rec));
		
		/*修改PAGE_GARBAGE*/
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

	page_create_write_log(frame, mtr);

	page = frame;

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

	/*create supremum*/
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

	page_header_set_field(page, PAGE_N_DIR_SLOTS, 2);
	page_header_set_ptr(page, PAGE_HEAP_TOP, heap_top);
	page_header_set_field(page, PAGE_N_HEAP, 2);
	page_header_set_ptr(page, PAGE_FREE, NULL);
	page_header_set_field(page, PAGE_GARBAGE, 0);
	page_header_set_ptr(page, PAGE_LAST_INSERT, NULL);
	page_header_set_field(page, PAGE_N_RECS, 0);
	page_set_max_trx_id(page, ut_dulint_zero);

	slot = page_dir_get_nth_slot(page, 0);
	page_dir_slot_set_rec(slot, infimum_rec);

	slot = page_dir_get_nth_slot(page, 1);
	page_dir_slot_set_rec(slot, supremum_rec);

	rec_set_next_offs(infimum_rec, (ulint)(supremum_rec - page)); 
	rec_set_next_offs(supremum_rec, 0);

	return page;
}

void page_copy_rec_list_end_no_locks(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr)
{
	page_cur_t	cur1;
	page_cur_t	cur2;
	rec_t*		sup;
	/*获得page的游标*/
	page_cur_position(rec, &cur1);
	if(page_cur_is_before_first(&cur1))
		page_cur_move_to_next(&cur1);

	/*设置new_page的游标*/
	page_cur_set_before_first(new_page, &cur2);

	/*进行记录copy*/
	sup = page_get_supremum_rec(page);
	while(sup != page_cur_get_rec(&cur1)){
		ut_a(page_cur_rec_insert(&cur2, page_cur_get_rec(&cur1), mtr));
		page_cur_move_to_next(&cur1);
		page_cur_move_to_next(&cur2);
	}
}

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

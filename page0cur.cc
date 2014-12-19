#include "page0cur.h"

#include "rem0cmp.h"
#include "mtr0log.h"
#include "log0recv.h"
#include "rem0types.h"

ulint page_cur_short_succ = 0;
ulint page_rnd = 976722341;

#ifdef PAGE_CUR_ADAPT

UNIV_INLINE ibool page_cur_try_search_shortcut(page_t* page, dtuple_t* tuple, 
	ulint* iup_matched_fields, ulint* iup_matched_bytes, ulint* ilow_matched_fields, ulint* ilow_matched_bytes, page_cur_t* cursor)
{
	int	cmp;
	rec_t*	rec;
	rec_t*	next_rec;
	ulint	low_match;
	ulint	low_bytes;
	ulint	up_match;
	ulint	up_bytes;

	ut_ad(dtuple_check_typed(tuple));

	rec = page_header_get_ptr(page, PAGE_LAST_INSERT);
	ut_ad(rec);
	ut_ad(page_rec_is_user_rec(rec));

	ut_pair_min(&low_match, &low_bytes, *ilow_matched_fields, *ilow_matched_bytes,*iup_matched_fields, *iup_matched_bytes);

	up_match = low_match;
	up_bytes = low_bytes;

	/*检查TUPLE是否超出范围*/
	cmp = page_cmp_dtuple_rec_with_match(tuple, rec, &low_match, &low_bytes);
	if(cmp == -1) /*rec是supremum*/
		return FALSE;

	next_rec = page_rec_get_next(rec);
	cmp = page_cmp_dtuple_rec_with_match(tuple, next_rec, &up_match, &up_bytes);
	if(cmp != -1)
		return FALSE;

	cursor->rec = rec;
	if(next_rec != page_get_supremum_rec(page)){
		*iup_matched_fields = up_match;
		*iup_matched_bytes = up_bytes;
	}

	*ilow_matched_fields = low_match;
	*ilow_matched_bytes = low_bytes;

	return TRUE;
}
#endif

void page_cur_search_with_match(page_t* page, dtuple_t* tuple, ulint mode, 
	ulint* iup_matched_fields, ulint* iup_matched_bytes, 
	ulint* ilow_matched_fields, ulint* ilow_matched_bytes, page_cur_t* cursor)
{
	ulint	up;	
	ulint	low;	
	ulint	mid;
	page_dir_slot_t* slot;
	rec_t*	up_rec;
	rec_t*	low_rec;
	rec_t*	mid_rec;
	ulint	up_matched_fields;
	ulint	up_matched_bytes;
	ulint	low_matched_fields;
	ulint	low_matched_bytes;
	ulint	cur_matched_fields;
	ulint	cur_matched_bytes;
	int	cmp;

	ut_ad(page && tuple && iup_matched_fields && iup_matched_bytes && ilow_matched_fields && ilow_matched_bytes && cursor);

	ut_ad(dtuple_validate(tuple));
	ut_ad(dtuple_check_typed(tuple));

	ut_ad((mode == PAGE_CUR_L) || (mode == PAGE_CUR_LE)
		|| (mode == PAGE_CUR_G) || (mode == PAGE_CUR_GE)
		|| (mode == PAGE_CUR_DBG));

#ifdef PAGE_CUR_ADAPT
	if ((page_header_get_field(page, PAGE_LEVEL) == 0)
		&& (mode == PAGE_CUR_LE)
		&& (page_header_get_field(page, PAGE_N_DIRECTION) > 3)
		&& (page_header_get_ptr(page, PAGE_LAST_INSERT))
		&& (page_header_get_field(page, PAGE_DIRECTION) == PAGE_RIGHT)) {

			if (page_cur_try_search_shortcut(page, tuple,
				iup_matched_fields, iup_matched_bytes,
				ilow_matched_fields, ilow_matched_bytes, cursor))
					return;
	}

	if (mode == PAGE_CUR_DBG)
		mode = PAGE_CUR_LE;
#endif	

	up_matched_fields  = *iup_matched_fields;
	up_matched_bytes   = *iup_matched_bytes;
	low_matched_fields = *ilow_matched_fields;
	low_matched_bytes  = *ilow_matched_bytes;

	low = 0;
	up = page_dir_get_n_slots(page) - 1;

	/*进行二分查找*/
	while(up - low > 1){
		mid = (low + up) / 2;
		slot = page_dir_get_nth_slot(page, mid);
		mid_rec = page_dir_slot_get_rec(slot);

		ut_pair_min(&cur_matched_fields, &cur_matched_bytes,
			low_matched_fields, low_matched_bytes,
			up_matched_fields, up_matched_bytes);

		cmp = cmp_dtuple_rec_with_match(tuple, mid_rec,
			&cur_matched_fields,
			&cur_matched_bytes);

		if(cmp == -1){
			low = mid;
			low_matched_fields = cur_matched_fields;
			low_matched_bytes = cur_matched_bytes;
		}
		else if(cmp == 1){
			up = mid;
			up_matched_fields = cur_matched_fields;
			up_matched_bytes = cur_matched_bytes; 
		}
		else if((mode == PAGE_CUR_G) || (mode == PAGE_CUR_LE)){
			low = mid;
			low_matched_fields = cur_matched_fields;
			low_matched_bytes = cur_matched_bytes;
		}
		else{
			up = mid;
			up_matched_fields = cur_matched_fields;
			up_matched_bytes = cur_matched_bytes;
		}
	}

	slot = page_dir_get_nth_slot(page, low);
	low_rec = page_dir_slot_get_rec(slot);

	slot = page_dir_get_nth_slot(page, up);
	up_rec = page_dir_slot_get_rec(slot);

	while(page_rec_get_next(low_rec) != up_rec){
		mid_rec = page_rec_get_next(low_rec);

		ut_pair_min(&cur_matched_fields, &cur_matched_bytes,
			low_matched_fields, low_matched_bytes,
			up_matched_fields, up_matched_bytes);

		cmp = cmp_dtuple_rec_with_match(tuple, mid_rec,
			&cur_matched_fields,
			&cur_matched_bytes);

		if (cmp == 1) {
			low_rec = mid_rec;
			low_matched_fields = cur_matched_fields;
			low_matched_bytes = cur_matched_bytes;

		} else if (cmp == -1) {
			up_rec = mid_rec;
			up_matched_fields = cur_matched_fields;
			up_matched_bytes = cur_matched_bytes; 

		} else if ((mode == PAGE_CUR_G) || (mode == PAGE_CUR_LE)) {
			low_rec = mid_rec;
			low_matched_fields = cur_matched_fields;
			low_matched_bytes = cur_matched_bytes;
		} else {
			up_rec = mid_rec;
			up_matched_fields = cur_matched_fields;
			up_matched_bytes = cur_matched_bytes;
		}
	}

	if(mode <= PAGE_CUR_GE)
		cursor->rec = up_rec;
	else
		cursor->rec = low_rec;

	*iup_matched_fields  = up_matched_fields;
	*iup_matched_bytes   = up_matched_bytes;
	*ilow_matched_fields = low_matched_fields;
	*ilow_matched_bytes  = low_matched_bytes;
}

/*随机取一条记录作为游标的指向*/
void page_cur_open_on_rnd_user_rec(page_t* page, page_cur_t* cursor)
{
	ulint rnd;
	rec_t* rec;

	if(page_get_n_recs(page) == 0){
		page_cur_position(page_get_infimum_rec(page), cursor);
		return;
	}

	pag_rnd += 87584577;
	rnd = page_rnd % page_get_n_recs(page);
	rec = page_get_infimum_rec(page);
	rec = page_rec_get_next(rec);
	while(rnd > 0){
		rec = page_rec_get_next(rec);
		rnd --;
	}

	page_cur_position(rec, cursor);
}
/*写入插入记录行为的mtr log*/
static void page_cur_insert_rec_write_log(rec_t* insert_rec, ulint rec_size, rec_t* cursor_rec, mtr_t* mtr)
{
	ulint	cur_rec_size;
	ulint	extra_size;
	ulint	cur_extra_size;
	ulint	min_rec_size;
	byte*	ins_ptr;
	byte*	cur_ptr;
	ulint	extra_info_yes;
	byte*	log_ptr;
	ulint	i;

	ut_a(rec_size < UNIV_PAGE_SIZE);

	/*打开一个日志写入的动态缓冲区*/
	log_ptr = mlog_open(mtr, 30 + MLOG_BUF_MARGIN);
	if(log_ptr == NULL)
		return ;

	extra_size = rec_get_extra_size(insert_rec);
	cur_extra_size = rec_get_extra_size(cursor_rec);

	ins_ptr = insert_rec - extra_size;

	i = 0;
	if(cur_extra_size == extra_size){
		min_rec_size = ut_min(cur_rec_size, rec_size);
		cur_ptr = cursor_rec - cur_rec_size;

		for(;;){
			if(i >= min_rec_size)
				break;
			else if(*ins_ptr == *cur_ptr){
				i ++;
				ins_ptr ++;
				cur_ptr ++;
			}
			else if(i < extra_size && i >= extra_size - REC_N_EXTRA_BYTES){
				i = extra_size;
				ins_ptr = insert_rec;
				cur_ptr = cursor_rec;
			}
			else 
				break;
		}
	}

	if(mtr_get_log_mode(mtr) != MTR_LOG_SHORT_INSERTS){
		/*写入一个mtr log的头信息*/
		log_ptr = mlog_write_initial_log_record_fast(insert_rec, MLOG_REC_INSERT, log_ptr, mtr);
		mach_write_to_2(log_ptr, cursor_rec - buf_frame_align(cursor_rec));/*写入记录偏移量*/
		log_ptr += 2;
	}

	if((rec_get_info_bits(insert_rec) != rec_get_info_bits(cursor_rec))
		|| (extra_size != cur_rec_size) || (rec_size != cur_rec_size))
		extra_info_yes = 1;
	else
		extra_info_yes = 0;

	log_ptr += mach_write_compressed(log_ptr, 2 * (rec_size - i) + extra_info_yes);
	if(extra_info_yes){
		mach_write_to_1(log_ptr, rec_get_info_bits(insert_rec));
		log_ptr ++;

		log_ptr += mach_write_compressed(log_ptr, extra_size);
		log_ptr += mach_write_compressed(log_ptr, i);

		ut_a(i < UNIV_PAGE_SIZE);
		ut_a(extra_size < UNIV_PAGE_SIZE);
	}

	/*当前mtr log保存*/
	if(rec_size - i < MLOG_BUF_MARGIN){
		ut_memcpy(log_ptr, ins_ptr, rec_size - 1);
		log_ptr += rec_size - 1;

		mlog_close(mtr, log_ptr);
	}
	else{
		mlog_close(mtr, log_ptr);

		ut_a(rec_size - i < UNIV_PAGE_SIZE);
		/*另其一个mtr log行来做保存*/
		if(rec_size - i >= MLOG_BUF_MARGIN)
			mlog_catenate_string(mtr, ins_ptr, rec_size - i);
	}
}

/*通过解析mtr log向page中插入一条记录,与page_cur_insert_rec_write_log对应的*/
byte* page_cur_parse_insert_rec(ibool is_short, byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	ulint	extra_info_yes;
	ulint	offset;
	ulint	origin_offset;
	ulint	end_seg_len;
	ulint	mismatch_index;
	byte	buf1[1024];
	byte*	buf;
	ulint	info_bits;
	page_cur_t cursor;
	rec_t*	cursor_rec;

	if(!is_short){
		if(end_ptr < ptr + 2)
			return NULL;

		/*读记录的page偏移*/
		offset = mach_read_from_2(ptr);
		/*日志出错了*/
		if(offset >= UNIV_PAGE_SIZE){ 
			recv_sys->found_corrupt_log = TRUE;
			return NULL;
		}

		ptr += 2;
	}

	ptr = mach_parse_compressed(ptr, end_ptr, &end_seg_len);
	if(ptr == NULL)
		return NULL;

	extra_info_yes = end_seg_len & 0x01;
	end_seg_len = end_seg_len / 2;
	if(end_seg_len >= UNIV_PAGE_SIZE){
		recv_sys->found_corrupt_log = TRUE;
		return NULL;
	}

	if(extra_info_yes){
		if(end_ptr < ptr + 1)
			return NULL;

		info_bits = mach_read_from_1(ptr);
		ptr ++;
		ptr = mach_parse_compressed(ptr, end_ptr, &origin_offset);
		if(ptr == NULL)
			return NULL;

		ut_a(origin_offset < UNIV_PAGE_SIZE);
		ptr = mach_parse_compressed(ptr, end_ptr, &mismatch_index);
		if(ptr == NULL)
			return NULL;

		ut_a(mismatch_index < UNIV_PAGE_SIZE);
	}

	if(end_ptr < ptr + end_seg_len)
		return NULL;

	if(page == NULL)
		return ptr + end_seg_len;

	/*定位cur_rec位置*/
	if(is_short)
		cursor_rec = page_rec_get_prev(page_get_supremum_rec(page));
	else
		cursor_rec = page + offset;

	if(extra_info_yes == 0){
		info_bits = rec_get_info_bits(cursor_rec);
		origin_offset = rec_get_extra_size(cursor_rec);
		mismatch_index = rec_get_size(cursor_rec) - end_seg_len;
	}

	if(mismatch_index + end_seg_len < 1024)
		buf = buf1;
	else
		buf = mem_alloc(mismatch_index + end_seg_len);

	ut_a(mismatch_index < UNIV_PAGE_SIZE);
	/*记录数据复制*/
	ut_memcpy(buf, rec_get_start(cursor_rec), mismatch_index);
	ut_memcpy(buf + mismatch_index, ptr, end_seg_len);

	rec_set_info_bits(buf + origin_offset, info_bits);

	page_cur_position(cursor_rec, &cursor);
	page_cur_rec_insert(&cursor, buf + origin_offset, mtr);

	if(mismatch_index + end_seg_len >= 1024)
		mem_free(buf);

	return ptr + end_seg_len;
}

rec_t* page_cur_insert_rec_low(page_cur_t* cursor, dtuple_t* tuple, ulint data_size, rec_t* rec, mtr_t* mtr)
{
	byte*		insert_buf = NULL;
	ulint		rec_size;
	byte*		page;
	rec_t*		last_insert;
	rec_t*		insert_rec;
	ulint		heap_no;
	rec_t*		current_rec;
	rec_t*		next_cur;
	ulint		owner_slot;
	rec_t*		owner_rec;
	ulint		n_owned;

	ut_ad(cursor && mtr);
	ut_ad(tuple || rec);
	ut_ad(!(tuple && rec));
	ut_ad(rec || dtuple_check_typed(tuple));
	ut_ad(rec || (dtuple_get_data_size(tuple) == data_size));

	page = page_cur_get_page(cursor);
	
	/*获取page上对应记录的长度*/
	if(tuple != NULL){
		rec_size = data_size + rec_get_converted_extra_size(data_size, dtuple_get_n_fields(tuple));
	}
	else{
		rec_size = rec_get_size(rec);
	}

	/*在page上找一个空闲能存储rec的缓冲区*/
	insert_buf = page_mem_alloc(page, rec_size, &heap_no);
	if(insert_buf == NULL)
		return NULL;

	/*创建记录*/
	if(tuple != NULL)
		insert_rec = rec_convert_dtuple_to_rec_low(insert_buf, tuple, data_size);
	else
		insert_rec = rec_copy(insert_buf, rec);

	ut_ad(insert_rec);
	ut_ad(rec_size == rec_get_size(insert_rec));

	current_rec = cursor->rec;

	next_rec = page_rec_get_next(current_rec);
	page_rec_set_next(insert_rec, next_rec);
	page_rec_set_next(current_rec, insert_rec);

	page_header_set_field(page, PAGE_N_RECS, 1 + page_get_n_recs(page));

	rec_set_n_owned(insert_rec, 0);
	rec_set_heap_no(insert_rec, heap_no);

	last_insert = page_header_get_ptr(page, PAGE_LAST_INSERT);
	/*增加游标方向的计数*/
	if(last_insert == NULL){
		page_header_set_field(page, PAGE_DIRECTION, PAGE_NO_DIRECTION);
		page_header_set_field(page, PAGE_N_DIRECTION, 0);
	}
	else if((last_insert == current_rec) && (page_header_get_field(page, PAGE_DIRECTION) != PAGE_LEFT)){
		page_header_set_field(page, PAGE_DIRECTION, PAGE_RIGHT);
		page_header_set_field(page, PAGE_N_DIRECTION,
		page_header_get_field(page, PAGE_N_DIRECTION) + 1);
	}
	else{
		page_header_set_field(page, PAGE_DIRECTION, PAGE_NO_DIRECTION);
		page_header_set_field(page, PAGE_N_DIRECTION, 0);
	}

	page_header_set_ptr(page, PAGE_LAST_INSERT, insert_rec);

	owner_rec = page_rec_find_owner_rec(insert_rec);
	n_owned = rec_get_n_owned(owner_rec);
	rec_set_n_owned(owner_rec, n_owned + 1);

	if(n_owned == PAGE_DIR_SLOT_MAX_N_OWNED){
		owner_slot = page_dir_find_owner_slot(owner_rec);
		page_dir_split_slot(page, owner_slot);
	}

	/*记录一条mtr log*/
	page_cur_insert_rec_write_log(insert_rec, rec, rec_size, current_rec, mtr);

	return insert_rec;
}

/*构建一个mtr log来记录拷贝记录的日志*/
UNIV_INLINE byte* page_copy_rec_list_to_created_page_write_log(page_t* page, mtr_t* mtr)
{
	byte*	log_ptr;

	mlog_write_initial_log_record(page, MLOG_LIST_END_COPY_CREATED, mtr);
	log_ptr = mlog_open(mtr, 4);
	mlog_close(mtr, log_ptr + 4);

	return log_ptr;
}

byte* page_parse_copy_rec_list_to_created_page(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	byte*	rec_end;
	ulint	log_data_len;

	if(ptr + 4 > end_ptr)
		return NULL;

	log_data_len = mach_read_from_4(ptr);
	ptr += 4;

	rec_end = ptr + log_data_len;
	if(rec_end > end_ptr)
		return ;

	if(!page)
		return rec_end;

	/*对mtr log进行解析*/
	while(ptr < rec_end)
		ptr =  page_cur_parse_insert_rec(TRUE, ptr, end_ptr, page, mtr);

	ut_a(ptr == rec_end);

	page_header_set_ptr(page, PAGE_LAST_INSERT, NULL);
	page_header_set_field(page, PAGE_DIRECTION, PAGE_NO_DIRECTION);
	page_header_set_field(page, PAGE_N_DIRECTION, 0);

	return rec_end;
}

/*将page中的记录copy到一个新建立的page上*/
void page_copy_rec_list_end_to_created_page(page_t* new_page, page_t* page, rec_t* rec, mtr_t* mtr)
{
	page_dir_slot_t* slot;
	byte*	heap_top;
	rec_t*	insert_rec;
	rec_t*	prev_rec;
	ulint	count;
	ulint	n_recs;
	ulint	slot_index;
	ulint	rec_size;
	ulint	log_mode;
	byte*	log_ptr;
	ulint	log_data_len;

	ut_ad(page_header_get_field(new_page, PAGE_N_HEAP) == 2);
	ut_ad(page != new_page);

	if(rec == page_get_infimum_rec(page))
		rec = page_rec_get_next(rec);

	if(rec == page_get_supremum_rec(page))
		return ;

	log_ptr = page_copy_rec_list_to_created_page_write_log(new_page, mtr);
	log_data_len = dyn_array_get_data_size(&(mtr->log));

	log_mode = mtr_set_log_mode(mtr, MTR_LOG_SHORT_INSERTS);
	prev_rec = page_get_infimum_rec(new_page);
	heap_top = new_page + PAGE_SUPREMUM_END;

	count = 0;
	slot_index = 0;
	n_recs = 0;

	while(rec != page_get_supremum_rec(page)){
		insert_rec = rec_copy(heap_top, rec);
		rec_set_next_offs(prev_rec, insert_rec - new_page);

		rec_set_n_owned(insert_rec, 0);
		rec_set_heap_no(insert_rec, 2 + n_recs);

		rec_size = rec_get_size(insert_rec);
		heap_top = heap_top + rec_size;

		ut_ad(heap_top < new_page + UNIV_PAGE_SIZE);
		count ++;
		n_recs ++;

		if(count == (PAGE_DIR_SLOT_MAX_N_OWNED + 1) / 2){
			slot_index ++;
			slot = page_dir_get_nth_slot(new_page, slot_index);
			
			page_dir_slot_set_rec(slot, insert_rec);
			page_dir_slot_set_n_owned(slot, count);

			count = 0;
		}

		page_cur_insert_rec_write_log(insert_rec, rec_size, prev_rec, mtr);
		prev_rec = insert_rec;
		rec = page_rec_get_next(rec);
	}

	if((slot_index > 0) && (count + 1 + (PAGE_DIR_SLOT_MAX_N_OWNED + 1) / 2) <= PAGE_DIR_SLOT_MAX_N_OWNED){
		count += (PAGE_DIR_SLOT_MAX_N_OWNED + 1) / 2;
		page_dir_slot_set_n_owned(slot);

		slot_index --;
	}

	log_data_len = dyn_array_get_data_size(&(mtr->log)) - log_data_len;
	ut_a(log_data_len < 100 * UNIV_PAGE_SIZE);

	mach_write_to_4(log_ptr, log_data_len);
	rec_set_next_offs(insert_rec, PAGE_SUPREMUM);

	slot = page_dir_get_nth_slot(new_page, 1 + slot_index);

	page_dir_slot_set_rec(slot, page_get_supremum_rec(new_page));
	page_dir_slot_set_n_owned(slot, count + 1);

	page_header_set_field(new_page, PAGE_N_DIR_SLOTS, 2 + slot_index);
	page_header_set_ptr(new_page, PAGE_HEAP_TOP, heap_top);
	page_header_set_field(new_page, PAGE_N_HEAP, 2 + n_recs);
	page_header_set_field(new_page, PAGE_N_RECS, n_recs);

	page_header_set_ptr(new_page, PAGE_LAST_INSERT, NULL);
	page_header_set_field(new_page, PAGE_DIRECTION, PAGE_NO_DIRECTION);
	page_header_set_field(new_page, PAGE_N_DIRECTION, 0);

	mtr_set_log_mode(mtr, log_mode);
}

UNIV_INLINE void page_cur_delete_rec_write_log(rec_t* cursor_rec, mtr_t* mtr)
{
	mlog_write_initial_log_record(cursor_rec, MLOG_REC_DELETE, mtr);
	mlog_catenate_ulint(mtr, cursor_rec - buf_frame_align(cursor_rec), MLOG_2BYTES);
}

byte* page_cur_parse_delete_rec(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	ulint		offset;
	page_cur_t	cursor;

	if(end_ptr < ptr + 2)
		return NULL;

	ut_a(offset <= UNIV_PAGE_SIZE);

	if(page){
		page_cur_position(page + offset, &cursor);
		page_cur_delete_rec(&cursor, mtr);
	}

	return ptr;
}

void page_cur_delete_rec(page_cur_t* cursor, mtr_t* mtr)
{
	page_dir_slot_t* cur_dir_slot;
	page_dir_slot_t* prev_slot;
	page_t*		page;
	rec_t*		current_rec;
	rec_t*		prev_rec	= NULL;
	rec_t*		next_rec;
	ulint		cur_slot_no;
	ulint		cur_n_owned;
	rec_t*		rec;

	ut_ad(cursor && mtr);

	page = page_cur_get_page(cursor);
	current_rec = cursor->rec;

	ut_ad(current_rec != page_get_supremum_rec(page));
	ut_ad(current_rec != page_get_infimum_rec(page));

	cur_slot_no = page_dir_find_owner_slot(current_rec);
	cur_dir_slot = page_dir_get_nth_slot(page, cur_slot_no);
	cur_n_owned = page_dir_slot_set_n_owned(cur_dir_slot);

	page_cur_delete_rec_write_log(current_rec, mtr);

	page_header_set_ptr(page, PAGE_LAST_INSERT, NULL);
	buf_frame_modify_clock_inc(page);

	ut_ad(cur_slot_no > 0);
	prev_slot = page_dir_get_nth_slot(page, cur_slot_no - 1);
	rec = page_dir_slot_get_rec(prev_slot);
	/*查找要删除记录的前一条记录*/
	while(current_rec != rec){
		prev_rec = rec;
		rec = page_rec_get_next(rec);
	}
	/*查找要删除记录的后一条记录*/
	page_cur_move_to_next(cursor);
	next_rec = cursor->rec;

	/*进行删除*/
	page_rec_set_next(prev_rec, next_rec);
	page_header_set_field(page, PAGE_N_RECS, (ulint)(page_get_n_recs(page) - 1));

	ut_ad(PAGE_DIR_SLOT_MIN_N_OWNED >= 2);
	ut_ad(cur_n_owned > 1);

	if(current_rec == page_dir_slot_get_rec(cur_dir_slot)){
		page_dir_slot_set_rec(cur_dir_slot, prev_rec);
	}

	page_dir_slot_set_n_owned(cur_dir_slot, cur_n_owned - 1);

	/*将删除的记录空间进行回收*/
	page_mem_free(page, current_rec);

	if(cur_n_owned <= PAGE_DIR_SLOT_MIN_N_OWNED)
		page_dir_balance_slot(page);
}










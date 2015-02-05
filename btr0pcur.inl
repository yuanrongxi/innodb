
/*获得pcursor的rel_pos*/
UNIV_INLINE ulint btr_pcur_get_rel_pos(btr_pcur_t* cursor)
{
	ut_ad(cursor);
	ut_ad(cursor->old_rec);
	ut_ad(cursor->old_stored == BTR_PCUR_OLD_STORED);
	ut_ad(cursor->pos_state == BTR_PCUR_WAS_POSITIONED || cursor->pos_state == BTR_PCUR_IS_POSITIONED);

	return cursor->rel_pos;
}

/*设置mtr*/
UNIV_INLINE void btr_pcur_set_mtr(btr_pcur_t* cursor, mtr_t* mtr)
{
	ut_ad(cursor);

	cursor->mtr = mtr;
}
/*获取mtr*/
UNIV_INLINE mtr_t* btr_pcur_get_mtr(btr_pcur_t* cursor)
{
	ut_ad(cursor);

	return cursor->mtr;
}

/*获得btree cursor*/
UNIV_INLINE btr_cur_t* btr_pcur_get_btr_cur(btr_pcur_t* cursor)
{
	return &(cursor->btr_cur);
}

/*获得pcursor对应的page cursor*/
UNIV_INLINE page_cur_t* btr_pcur_get_page_cur(btr_pcur_t* cursor)
{
	return btr_cur_get_page_cur(&(cursor->btr_cur));
}

/*获得pcursor对应的page*/
UNIV_INLINE page_t* btr_pcur_get_page(btr_pcur_t* cursor)
{
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);

	return page_cur_get_page(btr_pcur_get_page_cur(cursor));
}

/*获得pcursor对应的记录*/
UNIV_INLINE rec_t* btr_pcur_get_rec(btr_pcur_t* cursor, mtr_t* mtr)
{
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	return page_cur_get_rec(btr_pcur_get_page_cur(cursor));
}

/*获得pcursor的up_match*/
UNIV_INLINE ulint btr_pcur_get_up_match(btr_pcur_t* cursor)
{
	btr_cur_t* btr_cursor;

	ut_ad((cursor->pos_state == BTR_PCUR_WAS_POSITIONED) || (cursor->pos_state == BTR_PCUR_IS_POSITIONED));

	btr_cursor = btr_pcur_get_btr_cur(cursor);

	ut_ad(btr_cursor->up_match != ULINT_UNDEFINED);

	return btr_cursor->up_match;
}

/*获得pcursor的low_match*/
UNIV_INLINE ulint btr_pcur_get_low_match(btr_pcur_t* cursor)
{
	btr_cur_t*	btr_cursor;

	ut_ad((cursor->pos_state == BTR_PCUR_WAS_POSITIONED) || (cursor->pos_state == BTR_PCUR_IS_POSITIONED));

	btr_cursor = btr_pcur_get_btr_cur(cursor);

	return btr_cursor->low_match;
}

/*判断pcursor是否指向对应的页的最后一条记录(supremum)*/
UNIV_INLINE ibool btr_pcur_is_after_last_on_page(btr_pcur_t* cursor, mtr_t* mtr)
{
	UT_NOT_USED(mtr);
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	return page_cur_is_after_last(btr_pcur_get_page_cur(cursor));
}

/*判断pcursor是否指向对应的页的第一条记录(Infimum)*/
UNIV_INLINE ibool btr_pcur_is_before_first_on_page(btr_pcur_t* cursor, mtr_t* mtr)
{
	UT_NOT_USED(mtr);
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	return page_cur_is_before_first(btr_pcur_get_page_cur(cursor));
}

/*pcursor指向对应page的有效记录上，除infimum和supremum之外的记录*/
UNIV_INLINE ibool btr_pcur_is_on_user_rec(btr_pcur_t* cursor, mtr_t* mtr)
{
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	if ((btr_pcur_is_before_first_on_page(cursor, mtr))
		|| (btr_pcur_is_after_last_on_page(cursor, mtr))) 
			return FALSE;

	return TRUE;
}

/*判断cursor是否指向的是BTree中所在层的最前面一条记录上*/
UNIV_INLINE ibool btr_pcur_is_before_first_in_tree(btr_pcur_t* cursor, mtr_t* mtr)
{
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	if(btr_page_get_prev(btr_pcur_get_page(cursor), mtr) != FIL_NULL)
		return FALSE;

	return page_cur_is_before_first(btr_pcur_get_page_cur(cursor));
}

/*判断cursor是否指向的是BTree中所在层的最后面一条记录上*/
UNIV_INLINE ibool btr_pcur_is_after_last_in_tree(btr_pcur_t* cursor, mtr_t* mtr)
{
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	if (btr_page_get_next(btr_pcur_get_page(cursor), mtr) != FIL_NULL)
		return FALSE;

	return page_cur_is_after_last(btr_pcur_get_page_cur(cursor));
}

/*pcursor向下一条记录移动，并指向下一条记录*/
UNIV_INLINE void btr_pcur_move_to_next_on_page(btr_pcur_t* cursor, mtr_t* mtr)
{
	UT_NOT_USED(mtr);
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	page_cur_move_to_next(btr_pcur_get_page_cur(cursor));

	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/*pcursor向上一条记录移动，并指向上一条记录*/
UNIV_INLINE void btr_pcur_move_to_prev_on_page(btr_pcur_t* cursor, mtr_t* mtr)
{
	UT_NOT_USED(mtr);
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	page_cur_move_to_prev(btr_pcur_get_page_cur(cursor));

	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/*将pcursor移动到下一条记录上，可以夸页,不会指向伪记录（infimum或者supremum）*/
UNIV_INLINE ibool btr_pcur_move_to_next_user_rec(btr_pcur_t* cursor, mtr_t* mtr)
{
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);
	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

loop:
	if(btr_pcur_is_after_last_on_page(cursor, mtr)){
		if(btr_pcur_is_after_last_in_tree(cursor, mtr)) /*已经到btree cursor指向节点所在层的最后一条记录，不能向后移动了*/
			return FALSE;

		/*将pcursor移动到下一页上*/
		btr_pcur_move_to_next_page(cursor, mtr);
	}
	else
		btr_pcur_move_to_next_on_page(cursor, mtr);

	/*判断cursor指向的记录是否是有效记录,如果不是有效记录，就需要移动*/
	if(btr_pcur_is_on_user_rec(cursor, mtr))
		return TRUE;

	goto loop;
}

/*将pcursor移动到下一条记录上，可以夸页,会指向伪记录（infimum或者supremum）*/
UNIV_INLINE ibool btr_pcur_move_to_next(btr_pcur_t* cursor, mtr_t* mtr)
{
	ut_ad(cursor->pos_state == BTR_PCUR_IS_POSITIONED);
	ut_ad(cursor->latch_mode != BTR_NO_LATCHES);

	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

	if(btr_pcur_is_after_last_on_page(cursor, mtr)){
		if(btr_pcur_is_before_first_in_tree(cursor, mtr)) /*已经到末尾了*/
			return FALSE;

		/*直接移向下一页的infimum上*/
		btr_pcur_move_to_next_page(cursor, mtr);

		return TRUE;
	}

	btr_pcur_move_to_next_on_page(cursor, mtr);

	return TRUE;
}

UNIV_INLINE void btr_pcur_commit(btr_pcur_t* pcur)
{
	ut_a(pcur->pos_state == BTR_PCUR_IS_POSITIONED);

	pcur->latch_mode = BTR_NO_LATCHES;
	mtr_commit(pcur->mtr);
	pcur->pos_state = BTR_PCUR_WAS_POSITIONED;
}

/*与btr_pcur_commit功能相似，唯一不同的是可以指定mtr*/
UNIV_INLINE void btr_pcur_commit_specify_mtr(btr_pcur_t* pcur, mtr_t* mtr)
{
	ut_a(pcur->pos_state == BTR_PCUR_IS_POSITIONED);

	pcur->latch_mode = BTR_NO_LATCHES;
	mtr_commit(mtr);
	pcur->pos_state = BTR_PCUR_WAS_POSITIONED;
}

UNIV_INLINE void btr_pcur_detach(btr_pcur_t* pcur)
{
	ut_a(pcur->pos_state == BTR_PCUR_IS_POSITIONED);

	pcur->latch_mode = BTR_NO_LATCHES;
	pcur->pos_state = BTR_PCUR_WAS_POSITIONED;
}

/*判断latch mode是否已经detach*/
UNIV_INLINE ibool btr_pcur_is_detached(btr_pcur_t* pcur)
{
	if(pcur->latch_mode == BTR_NO_LATCHES)
		return TRUE;

	return FALSE;
}

/*初始化一个pcursor*/
UNIV_INLINE void btr_pcur_init(btr_pcur_t* pcur)
{
	pcur->old_stored = BTR_PCUR_OLD_NOT_STORED;
	pcur->old_rec_buf = NULL;
	pcur->old_rec = NULL;
}

/*构建一个pcursor*/
UNIV_INLINE void btr_pcur_open(dict_index_t* index, dtuple_t* tuple, ulint mode, 
	ulint latch_mode, btr_pcur_t* cursor, mtr_t* mtr)
{
	btr_cur_t* btr_cursor;
	btr_pcur_init(cursor);

	cursor->latch_mode = latch_mode;
	cursor->search_mode = mode;
	
	btr_cursor = btr_pcur_get_btr_cur(cursor);
	/*定位到tuple的ROW KEY指向的btree cursor*/
	btr_cur_search_to_nth_level(index, 0, tuple, mode, latch_mode, btr_cursor, 0, mtr);

	cursor->pos_state = BTR_PCUR_IS_POSITIONED;
}

/*不需要进行初始化pcursor，构建一个pcursor,指向tuple在BTREE上的位置*/
UNIV_INLINE void btr_pcur_open_with_no_init(dict_index_t* index, dtuple_t* tuple, ulint mode, ulint latch_mode,
	btr_pcur_t* cursor, ulint has_search_latch, mtr_t* mtr)
{
	btr_cur_t* btr_cursor;

	cursor->latch_mode = latch_mode;
	cursor->search_mode = mode;
	
	btr_cursor = btr_pcur_get_btr_cur(cursor);

	btr_cur_search_to_nth_level(index, 0, tuple, mode, latch_mode, btr_cursor, has_search_latch, mtr);

	cursor->pos_state = BTR_PCUR_IS_POSITIONED;

	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

UNIV_INLINE void btr_pcur_open_at_index_side(ibool from_left, dict_index_t* index, ulint latch_mode, 
	btr_pcur_t* pcur, ibool do_init, mtr_t* mtr)
{
	pcur->latch_mode = latch_mode;
	if(from_left)
		pcur->search_mode = PAGE_CUR_G; /*大于*/
	else
		pcur->search_mode = PAGE_CUR_L; /*小于*/

	if(do_init)
		btr_pcur_init(pcur);

	/*根据搜索方向定位对应btree cursor指向的位置*/
	btr_cur_open_at_index_side(from_left, index, latch_mode, btr_pcur_get_btr_cur(pcur), mtr);

	pcur->pos_state = BTR_PCUR_IS_POSITIONED;
	pcur->old_stored = BTR_PCUR_OLD_NOT_STORED;
}

/*将pcursor随机指向btree上的一个位置*/
UNIV_INLINE void btr_pcur_open_at_rnd_pos(dict_index_t* index, ulint latch_mode, btr_pcur_t* cursor, mtr_t* mtr)
{
	cursor->latch_mode = latch_mode;
	cursor->search_mode = PAGE_CUR_G;

	btr_pcur_init(cursor);

	/*随机定位到btree上的一个位置*/
	btr_cur_open_at_rnd_pos(index, latch_mode, btr_pcur_get_btr_cur(cursor), mtr);

	cursor->pos_state = BTR_PCUR_IS_POSITIONED;
	cursor->old_stored = BTR_PCUR_OLD_STORED;
}

/*关闭pcursor*/
UNIV_INLINE void btr_pcur_close(btr_pcur_t* cursor)
{
	if(cursor->old_rec_buf != NULL){
		mem_free(cursor->old_rec_buf);
		cursor->old_rec = NULL;
		cursor->old_rec_buf = NULL;
	}

	cursor->btr_cur.page_cur.rec = NULL;
	cursor->old_rec = NULL;
	cursor->old_stored = BTR_PCUR_OLD_NOT_STORED;

	cursor->latch_mode = BTR_NO_LATCHES;
	cursor->pos_state = BTR_PCUR_NOT_POSITIONED;
}


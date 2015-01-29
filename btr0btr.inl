#include "mach0data.h"
#include "mtr0mtr.h"
#include "mtr0log.h"

#define BTR_MAX_NODE_LEVEL	50


UNIV_INLINE page_t* btr_page_get(ulint space, ulint page_no, ulint mode, mtr_t* mtr)
{
	page_t* page;
	
	page = buf_page_get(space, page_no, mode, mtr);

#ifdef UNIV_SYNC_DEBUG
	if (mode != RW_NO_LATCH)
		buf_page_dbg_add_level(page, SYNC_TREE_NODE);
#endif

	return page;
}

/*设置页的B-TREE的索引ID,会记录到redo log中*/
UNIV_INLINE void btr_page_set_index_id(page_t* page, dulint id, mtr_t* mtr)
{
	mlog_write_dulint(page + PAGE_HEADER + PAGE_INDEX_ID, id, MLOG_8BYTES, mtr);
}

UNIV_INLINE ulint btr_page_get_level_low(page_t* page)
{
	ulint level;

	ut_ad(page);

	level = mach_read_from_2(page + PAGE_HEADER + PAGE_LEVEL);
	ut_ad(level <= BTR_MAX_NODE_LEVEL);

	return level;
}

UNIV_INLINE ulint btr_page_get_level(page_t* page, mtr_t* mtr)
{
	ut_ad(page && mtr);

	return btr_page_get_level_low(page);
}

/*设置page所在的B-TREE主索引的层高*/
UNIV_INLINE void btr_page_set_level(page_t* page, ulint level, mtr_t* mtr)
{
	ut_ad(page && mtr);
	ut_ad(level <= BTR_MAX_NODE_LEVEL);

	mlog_write_ulint(page + PAGE_HEADER + PAGE_LEVEL, level, MLOG_2BYTES, mtr);
}

/*获取下一个page指向的下一个PAGE no,因为BTREE是叶子节点是一个双向链表*/
UNIV_INLINE ulint btr_page_get_next(page_t* page, mtr_t* mtr)
{
	ut_ad(page && mtr);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page),MTR_MEMO_PAGE_X_FIX)
		|| mtr_memo_contains(mtr, buf_block_align(page),MTR_MEMO_PAGE_S_FIX));

	return mach_read_from_4(page + FIL_PAGE_NEXT);
}

/*设置page下一个页的page no*/
UNIV_INLINE void btr_page_set_next(page_t* page, ulint next, mtr_t* mtr)
{
	ut_ad(page && mtr);

	mlog_write_ulint(page + FIL_PAGE_NEXT, next, MLOG_4BYTES, mtr);
}

UNIV_INLINE ulint btr_page_get_prev(page_t* page, mtr_t* mtr)
{
	ut_ad(page && mtr);

	return(mach_read_from_4(page + FIL_PAGE_PREV));
}

UNIV_INLINE void btr_page_set_prev(page_t* page, ulint prev, mtr_t* mtr)
{
	ut_ad(page && mtr);

	mlog_write_ulint(page + FIL_PAGE_PREV, next, MLOG_4BYTES, mtr);
}

/*从node ptr中获取对应孩子的PAGE NO*/
UNIV_INLINE ulint btr_node_ptr_get_child_page_no(rec_t* rec)
{
	ulint	n_fields;
	byte*	field;
	ulint	len;

	n_fields = rec_get_n_fields(rec);
	field = rec_get_nth_field(rec, n_fields - 1, &len);

	ut_ad(len == 4);

	return mach_read_from_4(field);
}

/*释放这叶子节点所持有的latch*/
UNIV_INLINE void btr_leaf_page_release(page_t* page, ulint latch_mode)
{
	ut_ad(!mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_MODIFY));

	/*查询模式*/
	if(latch_mode == BTR_SEARCH_LEAF)
		mtr_memo_release(mtr, buf_block_align(page), MTR_MEMO_PAGE_S_FIX); /*释放mtr中的MTR_MEMO_PAGE_S_FIX latch*/
	else{/*修改模式*/
		ut_ad(latch_mode == BTR_MODIFY_LEAF);
		mtr_memo_release(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX); /*释放mtr中的MTR_MEMO_PAGE_X_FIX latch*/
	}
}


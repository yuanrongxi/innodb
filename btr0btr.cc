#include "btr0btr.h"
#include "fsp0fsp.h"
#include "page0page.h"
#include "btr0cur.h"
#include "btr0sea.h"
#include "btr0pcur.h"
#include "rem0cmp.h"
#include "lock0lock.h"
#include "ibuf0ibuf.h"

/*如果是顺序插入，这个页分裂的一个记录数因子*/
#define BTR_PAGE_SEQ_INSERT_LIMIT	5

static void			btr_page_create(page_t* page, dict_tree_t* tree, mtr_t* mtr);

UNIV_INLINE void	btr_node_ptr_set_child_page_no(rec_t* rec, ulint page_no, mtr_t* mtr);

static rec_t*		btr_page_get_father_node_ptr(dict_tree_t* tree, page_t* page, mtr_t* mtr); 

static void			btr_page_empty(page_t* page, mtr_t* mtr);

static ibool		btr_page_insert_fits(btr_cur_t* cursor, rec_t* split_rec, dtuple_t* tuple);

/**************************************************************************/
/*获得root node所在的page,并且会在上面加上x-latch*/
page_t* btr_root_get(dict_tree_t* tree, mtr_t* mtr)
{
	ulint	space;
	ulint	root_page_no;
	page_t*	root;

	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK)
		|| mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_S_LOCK));

	space = dict_tree_get_space(tree);
	root_page_no = dict_tree_get_page(tree);

	root = btr_page_get(space, root_page_no, RW_X_LATCH, mtr);

	return root;
}




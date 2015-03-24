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

/*获得B-TREE上rec位置的上一条记录*/
rec_t* btr_get_prev_user_rec(rec_t* rec, mtr_t* mtr)
{
	page_t*	page;
	page_t*	prev_page;
	ulint	prev_page_no;
	rec_t*	prev_rec;
	ulint	space;

	page = buf_frame_align(rec);

	if(page_get_infimum_rec(page) != rec){
		prev_rec = page_rec_get_prev(rec);

		if(page_get_infimum_rec(page) != prev_rec)
			return prev_rec;
	}

	prev_page_no = btr_page_get_prev(page, mtr);
	space = buf_frame_get_space_id(page);

	if(prev_page_no != FIL_NULL){
		prev_page = buf_page_get_with_no_latch(space, prev_page_no, mtr);

		/*对持有latch的判断,一定会持有latch的*/
		ut_ad((mtr_memo_contains(mtr, buf_block_align(prev_page), MTR_MEMO_PAGE_S_FIX))
			|| (mtr_memo_contains(mtr, buf_block_align(prev_page), MTR_MEMO_PAGE_X_FIX)));

		prev_rec = page_rec_get_prev(page_get_supremum_rec(prev_page));

		return prev_rec;
	}

	return NULL;
}

/*获得B-TREE上rec位置的下一条记录*/
rec_t* btr_get_next_user_rec(rec_t* rec, mtr_t* mtr)
{
	page_t*	page;
	page_t*	next_page;
	ulint	next_page_no;
	rec_t*	next_rec;
	ulint	space;

	page = buf_frame_align(rec);
	if(page_get_supremum_rec(page) != rec){
		next_rec = page_rec_get_next(rec);
		if(page_get_supremum_rec(page) != next_rec)
			return next_rec;
	}

	next_page_no = btr_page_get_next(page, mtr);
	space = buf_frame_get_space_id(page);

	if(next_page_no != FIL_NULL){
		next_page = buf_page_get_with_no_latch(space, next_page_no, mtr);

		ut_ad((mtr_memo_contains(mtr, buf_block_align(next_page), MTR_MEMO_PAGE_S_FIX))
			|| (mtr_memo_contains(mtr, buf_block_align(next_page), MTR_MEMO_PAGE_X_FIX)));

		next_rec = page_rec_get_next(page_get_infimum_rec(page));

		return next_rec;
	}

	return NULL;
}

static void btr_page_create(page_t* page, dict_tree_t* tree, mtr_t* mtr)
{
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	/*在内存中构建一个页结构,并且构建逻辑结构和默认记录*/
	page_create(page, mtr);

	btr_page_set_index_id(page, tree->id, mtr);
}

/*在insert buffer上开辟一个页空间*/
static page_t* btr_page_alloc_for_ibuf(dict_tree_t* tree, mtr_t* mtr)
{
	fil_addr_t	node_addr;
	page_t*		root;
	page_t*		new_page;

	root = btr_root_get(tree, mtr);
	/*获得一个page需要存储的fil addr空间*/
	node_addr = flst_get_first(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, mtr);
	ut_a(node_addr.page != FIL_NULL);

	/*在buf中获得一个新页的空间，这个BUF和磁盘一一对应的*/
	new_page = buf_page_get(dict_tree_get_space(tree), node_addr.page, RW_X_LATCH, mtr);
	buf_page_dbg_add_level(new_page, SYNC_TREE_NODE_NEW);

	/*从空闲的磁盘队列中删除对应page的node addr*/
	flst_remove(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, new_page + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST_NODE, mtr);

	ut_ad(flst_validate(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, mtr));

	return new_page;
}

/*B-TREE上分配页空间*/
page_t* btr_page_alloc(dict_index_t* tree, ulint hint_page_no, byte file_direction, ulint level, mtr_t* mtr)
{
	fseg_header_t*	seg_header;
	page_t*		root;
	page_t*		new_page;
	ulint		new_page_no;

	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));

	/*直接在ibuf上分配一个页*/
	if(tree->type & DICT_IBUF)
		return btr_page_alloc_for_ibuf(tree, mtr);

	/*获得root节点对应的页*/
	root = btr_root_get(tree, mtr);
	if(level == 0)
		seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
	else
		seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;

	/*在对应的表空间中分配一个页ID*/
	new_page_no = fseg_alloc_free_page_general(seg_header, hint_page_no, file_direction, TRUE, mtr);
	if(new_page_no == FIL_NULL) /*为获得合法的页ID*/
		return NULL;

	/*在IBUF上获得一个页空间,这个页还未真正初始化,需要调用btr_page_create对其初始化*/
	new_page = buf_page_get(dict_tree_get_space(tree), new_page_no, RW_X_LATCH, mtr);

	return new_page;
}

/*获得index对应的B-TREE上page的数量*/
ulint btr_get_size(dict_index_t* index, ulint flag)
{
	fseg_header_t*	seg_header;
	page_t*		root;
	ulint		n;
	ulint		dummy;
	mtr_t		mtr;

	mtr_start(&mtr);
	/*对树加上一个s-latch*/
	mtr_s_lock(dict_tree_get_lock(index->tree), &mtr);
	
	root = btr_root_get(index->tree, &mtr);

	if(flag == BTR_N_LEAF_PAGES){ /*获取叶子节点个数，直接返回对应segment中页的数量即可*/
		seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
		fseg_n_reserved_pages(seg_header, &n, &mtr);
	}
	else if(flag == BTR_TOTAL_SIZE){ /*获得所有的叠加*/
		seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;
		n = fseg_n_reserved_pages(seg_header, &dummy, &mtr);

		seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
		n += fseg_n_reserved_pages(seg_header, &dummy, &mtr);
	}
	else{
		ut_a(0);
	}

	mtr_commit(&mtr);

	return n;
}

/*ibuf回收page空间*/
static void btr_page_free_for_ibuf(dict_tree_t* tree, page_t* page, mtr_t* mtr)
{
	page_t* root;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	root = btr_root_get(tree, mtr);
	/*直接添加到root的ibuf free list当中就行*/
	flst_add_first(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, page + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST_NODE, mtr);

	ut_a(flst_validate(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, mtr));
}

/*回收page空间*/
void btr_page_free_low(dict_tree_t* tree, page_t* page, ulint level, mtr_t* mtr)
{
	fseg_header_t*	seg_header;
	page_t*		root;
	ulint		space;
	ulint		page_no;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	buf_frame_modify_clock_inc(page);

	if(tree->type & DICT_IBUF){
		btr_page_free_for_ibuf(tree, page, mtr);
		return;
	}

	/*获得对应表空间的segment位置信息*/
	root = btr_root_get(tree, mtr);
	if(level == 0)
		seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;
	else
		seg_header = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;

	space = buf_frame_get_space_id(page);
	page_no = buf_frame_get_page_no(page);

	/*在表空间中释放页*/
	fseg_free_page(seg_header, space, page_no, mtr);
}

/*释放page空间*/
void btr_page_free(dict_index_t* tree, page_t* page, mtr_t* mtr)
{
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));
	level = btr_page_get_level(page, mtr);

	btr_page_free_low(tree, page, level, mtr);
}

/*设置child node的fil addr*/
UNIV_INLINE void btr_node_ptr_set_child_page_no(rec_t* rec, ulint page_no, mtr_t* mtr)
{
	ulint	n_fields;
	byte*	field;
	ulint	len;

	ut_ad(0 < btr_page_get_level(buf_frame_align(rec), mtr));

	n_fields = rec_get_n_fields(rec);
	field = rec_get_nth_field(rec, n_fields - 1, &len);

	ut_ad(len == 4);

	/*将page no写入到rec中的最后一列上*/
	mlog_write_ulint(field, page_no, MLOG_4BYTES, mtr);
}

/*通过node ptr记录获得对应的page页*/
static page_t* btr_node_ptr_get_child(rec_t* node_ptr, mtr_t* mtr)
{
	ulint	page_no;
	ulint	space;
	page_t*	page;

	space = buf_frame_get_space_id(node_ptr);
	page_no = btr_node_ptr_get_child_page_no(node_ptr);

	page = btr_page_get(space, page_no, RW_X_LATCH, mtr);
	
	return page;
}

/*返回page也所在节点的上一层节点的node ptr 记录*/
static rec_t* btr_page_get_father_for_rec(dict_tree_t* tree, page_t* page, rec_t* user_rec, mtr_t* mtr)
{
	mem_heap_t*	heap;
	dtuple_t*	tuple;
	btr_cur_t	cursor;
	rec_t*		node_ptr;

	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));
	ut_a(user_rec != page_get_supremum_rec(page));
	ut_a(user_rec != page_get_infimum_rec(page));

	ut_ad(dict_tree_get_page(tree) != buf_frame_get_page_no(page));

	heap = mem_heap_create(100);
	
	/*构建一个node ptr 记录,将page no写入tuple中*/
	tuple = dict_tree_build_node_ptr(tree, user_sec, 0, heap, btr_page_get_level(page, mtr));

	btr_cur_search_to_nth_level(UT_LIST_GET_FIRST(tree->tree_indexes), btr_page_get_level(page, mtr) + 1,
		tuple, PAGE_CUR_LE, BTR_CONT_MODIFY_TREE, 
		&cursor, 0, mtr);

	node_ptr = btr_cur_get_rec(&cursor);

	/*node ptr中的孩子节点page no 与page中的page no不同，说明有错误,输出错误信息*/
	if(btr_node_ptr_get_child_page_no(node_ptr) != buf_frame_get_page_no(page)){
		fprintf(stderr, "InnoDB: Dump of the child page:\n");
		buf_page_print(buf_frame_align(page));

		fprintf(stderr,"InnoDB: Dump of the parent page:\n");
		buf_page_print(buf_frame_align(node_ptr));

		fprintf(stderr,
			"InnoDB: Corruption of an index tree: table %s, index %s,\n"
			"InnoDB: father ptr page no %lu, child page no %lu\n",
			(UT_LIST_GET_FIRST(tree->tree_indexes))->table_name,
			(UT_LIST_GET_FIRST(tree->tree_indexes))->name,
			btr_node_ptr_get_child_page_no(node_ptr),
			buf_frame_get_page_no(page));

		page_rec_print(page_rec_get_next(page_get_infimum_rec(page)));
		page_rec_print(node_ptr);

		fprintf(stderr,
			"InnoDB: You should dump + drop + reimport the table to fix the\n"
			"InnoDB: corruption. If the crash happens at the database startup, see\n"
			"InnoDB: section 6.1 of http://www.innodb.com/ibman.html about forcing\n"
			"InnoDB: recovery. Then dump + drop + reimport.\n");
	}

	ut_a(btr_node_ptr_get_child_page_no(node_ptr) == buf_frame_get_page_no(page));

	mem_heap_free(heap);
}
/*获得page上一层节点node ptr*/
static rec_t* btr_page_get_father_node_ptr(dict_tree_t* tree, page_t* page, mtr_t* mtr)
{
	return btr_page_get_father_for_rec(tree, page, page_rec_get_next(page_get_infimum_rec(page)), mtr);
}

/*建立一个btree，并返回root node 的page ID*/
ulint btr_create(ulint type, ulint space, dulint index_id, mtr_t* mtr)
{
	ulint			page_no;
	buf_frame_t*	ibuf_hdr_frame;
	buf_frame_t*	frame;
	page_t*			page;

	if(type & DICT_IBUF){
		/*分配一个fil segment*/
		ibuf_hdr_frame = fseg_create(space, 0, IBUF_HEADER + IBUF_TREE_SEG_HEADER, mtr);
		buf_page_dbg_add_level(ibuf_hdr_frame, SYNC_TREE_NODE_NEW);

		ut_ad(buf_frame_get_page_no(ibuf_hdr_frame) == IBUF_HEADER_PAGE_NO);

		/*在ibuf_hdr_frame上分配一个页*/
		page_no = fseg_alloc_free_page(ibuf_hdr_frame + IBUF_HEADER + IBUF_TREE_SEG_HEADER, 
			IBUF_TREE_ROOT_PAGE_NO, FSP_UP, mtr);

		frame = buf_page_get(space, page_no, RW_X_LATCH, mtr);
	}
	else{
		/*在表空间上创建一个file segment，并位于root的PAGE_BTR_SEG_TOP中*/
		frame = fseg_create(space, 0, PAGE_HEADER + PAGE_BTR_SEG_TOP, mtr);
	}

	if(frame == NULL)
		return FIL_NULL;

	page_no = buf_frame_get_page_no(frame);
	buf_page_dbg_add_level(frame, SYNC_TREE_NODE_NEW);
	if(type & DICT_IBUF){
		ut_ad(page_no == IBUF_TREE_ROOT_PAGE_NO);
		/*初始化空闲的ibuf 磁盘列表，用于存储释放的PAGE*/
		flst_init(frame + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, mtr);
	}
	else{ /*如果不是ibuf的b tree,创建一个fil segment用于存储leaf page*/
		fseg_create(space, page_no, PAGE_HEADER + PAGE_BTR_SEG_LEAF, mtr);
		buf_page_dbg_add_level(frame, SYNC_TREE_NODE_NEW);
	}

	/*在fil segment上创建一个page的页逻辑结构*/
	page = page_create(frame, mtr);

	/*设置page的index id*/
	btr_page_set_index_id(page, index_id, mtr);
	/*设置LEVEL*/
	btr_page_set_level(page, 0, mtr);

	/*设置叶子page关联关系*/
	btr_page_set_next(page, FIL_NULL, mtr);
	btr_page_set_prev(page, FIL_NULL, mtr);

	/*做什么用的？？*/
	ibuf_reset_free_bits_with_type(type, page);

	ut_ad(page_get_max_insert_size(page, 2) > 2 * BTR_PAGE_MAX_REC_SIZE);

	return page_no;
}

/*释放整个btree的page,root page不做释放*/
void btr_free_but_not_root(ulint space, ulint root_page_no)
{
	ibool	finished;
	page_t*	root;
	mtr_t	mtr;

/*叶子节点释放*/
leaf_loop:
	mtr_start(&mtr);
	/*释放真个root对应的segment的page*/
	root = btr_page_get(space, root_page_no, RW_X_LATCH, &mtr);
	finished = fseg_free_step(root + PAGE_HEADER + PAGE_BTR_SEG_LEAF, &mtr);
	mtr_commit(&mtr);

	if(!finished)
		goto leaf_loop;

/*枝干节点释放,不会释放root对应的头页，头页中有fsegment的信息*/
top_loop:
	mtr_start(&mtr);

	root = btr_page_get(space, root_page_no, RW_X_LATCH, &mtr);	
	finished = fseg_free_step_not_header(root + PAGE_HEADER + PAGE_BTR_SEG_TOP, &mtr);

	mtr_commit(&mtr);
	if(!finished)
		goto top_loop;
}

/*释放btree对应的root page*/
void btr_free_root(ulint space, ulint root_page_no, mtr_t* mtr)
{
	ibool	finished;
	page_t*	root;

	root = btr_page_get(space, root_page_no, RW_X_LATCH, mtr);
	/*删除掉对应的自适应hash索引*/
	btr_search_drop_hash_index(root);

top_loop:
	finished = fseg_free_step(root + PAGE_HEADER + PAGE_BTR_SEG_TOP, mtr);
	if(!finished)
		goto top_loop;
}

static void btr_page_reorganize_low(ibool recovery, page_t* page, mtr_t* mtr)
{
	page_t*	new_page;
	ulint	log_mode;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));
	/*写入一条page reorganize的日志*/
	mlog_write_initial_log_record(page, MLOG_PAGE_REORGANIZE, mtr);

	/*关闭mini transcation log模式*/
	log_mode = mtr_set_log_mode(mtr, MTR_LOG_NONE);

	new_page = buf_frame_alloc();
	buf_frame_copy(new_page, page);

	/*如果不是在恢复redo log的过程，就删除掉对应的哈希索引*/
	if(!recovery)
		btr_search_drop_page_hash_index(page);

	/*重新在page空间上构建一个新的page逻辑结构*/
	page_create(page, mtr);

	/*把整个new page中的记录全部转移到page上，和buf_frame_copy不同，这个应该是逻辑拷贝*/
	page_copy_rec_list_end_no_locks(page, new_page, page_get_infimum_rec(new_page), mtr);

	/*设置二级索引对应操作的事务ID*/
	page_set_max_trx_id(page, page_get_max_trx_id(new_page));

	/*重组更新page对应的事务锁*/
	if(!recovery)
		lock_move_reorganize_page(page, new_page);

	/*释放掉临时的页*/
	buf_frame_free(new_page);

	/*恢复mini transcation log模式*/
	mtr_set_log_mode(mtr, log_mode);
}

/*在非redo过程中重组page*/
void btr_page_reorganize(page_t* page, mtr_t* mtr)
{
	btr_page_reorganize_low(FALSE, page, mtr);
}

/*在redo log恢复过程中重组page*/
byte* btr_parse_page_reorganize(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	ut_ad(ptr && end_ptr);

	if(page)
		btr_page_reorganize_low(TRUE, page, mtr);

	return ptr;
}

/*btree索引page清空*/
static void btr_page_empty(page_t* page, mtr_t* mtr)
{
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	btr_search_drop_page_hash_index(page);

	page_create(page, mtr);
}

rec_t* btr_root_raise_and_insert(btr_cur_t* cursor, dtuple_t* tuple, mtr_t* mtr)
{
	dict_tree_t*	tree;
	page_t*			root;
	page_t*			new_page;
	ulint			new_page_no;
	rec_t*			rec;
	mem_heap_t*		heap;
	dtuple_t*		node_ptr;
	ulint			level;	
	rec_t*			node_ptr_rec;
	page_cur_t*		page_cursor;

	root = btr_cur_get_page(cursor);
	tree = btr_cur_get_tree(cursor);

	ut_ad(dict_tree_get_page(tree) == buf_frame_get_page_no(root));
	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(root), MTR_MEMO_PAGE_X_FIX));

	btr_search_drop_page_hash_index(root);
	/*在表空间中分配一个页空间*/
	new_page = btr_page_alloc(tree, 0, FSP_NO_DIR, btr_page_get_level(root, mtr), mtr);

	/*在btree上创建一个新页*/
	btr_page_create(new_page, tree, mtr);

	level = btr_page_get_level(root, mtr);

	/*设置层高*/
	btr_page_set_level(new_page, level, mtr);
	btr_page_set_level(root, level + 1, mtr);
	/*将root中所有的记录移到new_page中*/
	page_move_rec_list_end(new_page, root, page_get_infimum_rec(root), mtr);
	/*事务锁转移*/
	lock_update_root_raise(new_page, root);

	heap = mem_heap_create(100);

	/*获得new page的第一条有效记录*/
	rec = page_rec_get_next(page_get_infimum_rec(new_page));
	new_page_no = buf_frame_get_page_no(new_page);

	node_ptr = dict_tree_build_node_ptr(tree, rec, new_page_no, heap, level);

	/*将root page重新分配空间*/
	btr_page_reorganize(root, mtr);	

	page_cursor = btr_cur_get_page_cur(cursor);

	/*将new page的第一条记录(node_ptr)插入到root中*/
	page_cur_set_before_first(root, page_cursor);
	node_ptr_rec = page_cur_tuple_insert(page_cursor, node_ptr, mtr);
	ut_ad(node_ptr_rec);

	btr_set_min_rec_mark(node_ptr_rec, mtr);

	mem_heap_free(heap);

	ibuf_reset_free_bits(UT_LIST_GET_FIRST(tree->tree_indexes), new_page);
	/*重新定位page cursor的指向的位置，也会改变btree cursor，btree_cursor将会指向new page上的对应记录*/
	page_cur_search(new_page, tuple, PAGE_CUR_LE, page_cursor);

	return btr_page_split_and_insert(cursor, tuple, mtr);
}

/*判断页是否可以向左聚集进行分裂，并确定分裂的位置,也就是说后面插入的记录范围可能都在左边*/
ibool btr_page_get_split_rec_to_left(btr_cur_t* cursor, rec_t** split_rec)
{
	page_t*	page;
	rec_t*	insert_point;
	rec_t*	infimum;

	page = btr_cur_get_page(cursor);
	insert_point = btr_cur_get_rec(cursor);

	if ((page_header_get_ptr(page, PAGE_LAST_INSERT) == page_rec_get_next(insert_point))
		&& (page_header_get_field(page, PAGE_DIRECTION) == PAGE_LEFT)
		&& ((page_header_get_field(page, PAGE_N_DIRECTION) >= BTR_PAGE_SEQ_INSERT_LIMIT)
		|| (page_header_get_field(page, PAGE_N_DIRECTION) + 1>= page_get_n_recs(page)))) {

			infimum = page_get_infimum_rec(page);

			/*直接从insert point处分裂，如果insert point和infimum太近，从它的下一条记录处分裂*/
			if ((infimum != insert_point) && (page_rec_get_next(infimum) != insert_point))
				*split_rec = insert_point;
			else
				*split_rec = page_rec_get_next(insert_point);

			return TRUE;
	}

	return FALSE;
}

/*判断页是否可以向右聚集进行分裂，并确定分裂的位置,也就是说后面插入的记录范围可能都在右边*/
ibool btr_page_get_split_rec_to_right(btr_cur_t* cursor, rec_t** split_rec)
{
	page_t*	page;
	rec_t*	insert_point;
	rec_t*	supremum;

	page = btr_cur_get_page(cursor);
	insert_point = btr_cur_get_rec(cursor);

		if ((page_header_get_ptr(page, PAGE_LAST_INSERT) == insert_point)
	    && (page_header_get_field(page, PAGE_DIRECTION) == PAGE_RIGHT)
	    && ((page_header_get_field(page, PAGE_N_DIRECTION) >= BTR_PAGE_SEQ_INSERT_LIMIT)
	     	|| (page_header_get_field(page, PAGE_N_DIRECTION) + 1 >= page_get_n_recs(page)))) {

	     	supremum = page_get_supremum_rec(page);
	    
			/*从insert_point后面第3条记录开始分裂,否则直接从insert point处分裂*/
		if ((page_rec_get_next(insert_point) != supremum) && (page_rec_get_next(page_rec_get_next(insert_point)) != supremum)
		    && (page_rec_get_next(page_rec_get_next(page_rec_get_next(insert_point))) != supremum)) {

			/* If there are >= 3 user records up from the insert point, split all but 2 off */
			*split_rec = page_rec_get_next(page_rec_get_next(page_rec_get_next(insert_point)));
		} 
		else 
	     	*split_rec = NULL;

		return TRUE;
	}

	return FALSE;
}

/*确定从page 中间进行分裂执行时的记录位置，一般是随机插入时会进行判断*/
static rec_t* btr_page_get_sure_split_rec(btr_cur_t* cursor, dtuple_t* tuple)
{
	page_t*	page;
	ulint	insert_size;
	ulint	free_space;
	ulint	total_data;
	ulint	total_n_recs;
	ulint	total_space;
	ulint	incl_data;
	rec_t*	ins_rec;
	rec_t*	rec;
	rec_t*	next_rec;
	ulint	n;

	page = btr_cur_get_page(cursor);
	/*获得插入记录的在磁盘中应该占用的空间*/
	insert_size = rec_get_converted_size(tuple);
	/*获得页的记录可用空间*/
	free_space = page_get_free_space_of_empty();

	total_data = page_get_data_size(page) + insert_size;
	total_n_recs = page_get_n_recs(page) + 1;
	ut_ad(total_n_recs >= 2);

	/*计算现在page中被占用的空间数,记录空间 + 记录索引槽slots*/
	total_space = total_data + page_dir_calc_reserved_space(total_n_recs);

	n = 0;
	incl_data = 0;
	/*tuple应该插入的位置*/
	ins_rec = btr_cur_get_rec(cursor);
	rec = page_get_infimum_rec(page);

	for(;;){
		if(rec == ins_rec) /*不需要分裂，tuple记录可以直接插入到page*/
			rec = NULL;
		else if(rec == NULL)
			rec = page_rec_get_next(ins_rec);
		else
			rec = page_rec_get_next(rec);
	}

	/*假设插入tuple，统计插入后的数据长度，然后根据这个长度进行分裂判断*/
	if(rec == NULL)
		incl_data += insert_size;
	else
		incl_data += rec_get_size(rec);

	n ++;

	/*从infimum到rec位置的占用空间总和大于总使用空间一半,可以在rec记录处分裂*/
	if(incl_data + page_dir_calc_reserved_space(n) >= total_space / 2){
		/*占用空间的总和小于页的可用空间*/
		if(incl_data + page_dir_calc_reserved_space(n) <= free_space){
			if(rec == ins_rec) /*从insert rec处分裂？还是不需要分裂*/
				next_rec = NULL;
			else if(rec == NULL) /*从ins_rec的下一条记录处分裂*/
				next_rec = page_rec_get_next(ins_rec);
			else
				next_rec = page_rec_get_next(rec);

			if(next_rec != page_get_supremum_rec(page))
				return next_rec;
		}

		return rec;
	}

	return NULL;
}

/*page页中间分裂，split rec是否可以作为分裂点*/
static ibool btr_page_insert_fits(btr_cur_t* cursor, rec_t* split_rec, dtuple_t* tuple)
{
	page_t*	page;
	ulint	insert_size;
	ulint	free_space;
	ulint	total_data;
	ulint	total_n_recs;
	rec_t*	rec;
	rec_t*	end_rec;

	page = btr_cur_get_page(cursor);

	insert_size = rec_get_converted_size(tuple);
	free_space = page_get_free_space_of_empty();

	total_data   = page_get_data_size(page) + insert_size;
	total_n_recs = page_get_n_recs(page) + 1;

	/*未指定分裂点，从页第一条记录到cursor指向的记录区间,确定分裂点区间*/
	if(split_rec == NULL){
		rec = page_rec_get_next(page_get_infimum_rec(page));
		end_rec = page_rec_get_next(btr_cur_get_rec(cursor));
	}
	else if(cmp_dtuple_rec(tuple, split_rec) >= 0){ /*指定分裂位置，从开始到指定记录位置，且tuple落在split_rec之后的位置*/
		rec = page_rec_get_next(page_get_infimum_rec(page));
		end_rec = split_rec;
	}
	else{ /*tuple 在split_rec之前*/
		rec = split_rec;
		end_rec = page_get_supremum_rec(page);
	}

	if (total_data + page_dir_calc_reserved_space(total_n_recs) <= free_space) 
		return TRUE;

	while(rec != end_rec){
		total_data -= rec_get_size(rec);
		total_n_recs --;

		/*能保证页存下相对应的数据*/
		if(total_data + page_dir_calc_reserved_space(total_n_recs) <= free_space)
			return TRUE;

		rec = page_rec_get_next(rec);
	}

	return FALSE;
}

/*将一个tuple插入到btree中的非叶子节点*/
void btr_insert_on_non_leaf_level(dict_tree_t* tree, ulint level, dtuple_t* tuple, mtr_t* mtr)
{
	big_rec_t*	dummy_big_rec;
	btr_cur_t	cursor;		
	ulint		err;
	rec_t*		rec;

	ut_ad(level > 0);

	btr_cur_search_to_nth_level(UT_LIST_GET_FIRST(tree->tree_indexes), level, tuple, 
		PAGE_CUR_LE, BTR_CONT_MODIFY_TREE, &cursor, 0, mtr);

	err = btr_cur_pessimistic_insert(BTR_NO_LOCKING_FLAG | BTR_KEEP_SYS_FLAG | BTR_NO_UNDO_LOG_FLAG, 
		&cursor, tuple, &rec, &dummy_big_rec, NULL, mtr);

	ut_a(err == DB_SUCCESS);
}

/*页从中间分裂后，先修改分裂后在父亲页上的node ptr记录，然后更改兄弟页之间的前后关联关系*/
static void btr_attach_half_pages(dict_tree_t* tree, page_t* page, rec_t* split_rec, page_t* new_page, ulint direction, mtr_t* mtr)
{
	ulint		space;
	rec_t*		node_ptr;
	page_t*		prev_page;
	page_t*		next_page;
	ulint		prev_page_no;
	ulint		next_page_no;
	ulint		level;
	page_t*		lower_page;
	page_t*		upper_page;
	ulint		lower_page_no;
	ulint		upper_page_no;
	dtuple_t*	node_ptr_upper;
	mem_heap_t* 	heap;

	/*对page的mtr log做判断*/
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(new_page), MTR_MEMO_PAGE_X_FIX));

	/*分裂方向从高到低*/
	if(direction == FSP_DOWN){
		lower_page_no = buf_frame_get_page_no(new_page);
		upper_page_no = buf_frame_get_page_no(page);
		lower_page = new_page;
		upper_page = page;
		/*获得指向page的node ptr*/
		node_ptr = btr_page_get_father_node_ptr(tree, page, mtr);
		/*将lower_page_no的信息替换原来的node ptr,原来的node ptr的值将作为新node ptr插入到level + 1层的页上*/
		btr_node_ptr_set_child_page_no(node_ptr, lower_page_no, mtr);
	}
	else{ /*从低到高*/
		/*原来的node ptr不变，再后面添加一条分裂出来的页的node ptr到level+1层上*/
		lower_page_no = buf_frame_get_page_no(page);
		upper_page_no = buf_frame_get_page_no(new_page);
		lower_page = page;
		upper_page = new_page;
	}

	heap = mem_heap_create(100);
	level = btr_page_get_level(page, mtr);

	/*构建一个分裂出来的node ptr*/
	node_ptr_upper = dict_tree_build_node_ptr(tree, split_rec, upper_page_no, heap, level);
	/*将记录插入到更高level + 1层*/
	btr_insert_on_non_leaf_level(tree, level + 1, node_ptr_upper, mtr);

	/*获得分裂前page的前后关系*/
	prev_page_no = btr_page_get_prev(page, mtr);
	next_page_no = btr_page_get_next(page, mtr);
	space = buf_frame_get_space_id(page);

	/*修改prev page中的隐射关系*/
	if(prev_page_no != FIL_NULL){
		prev_page = btr_page_get(space, prev_page_no, RW_X_LATCH, mtr); /*获得前一个page*/
		btr_page_set_next(prev_page, lower_page_no, mtr);
	}

	/*修改next page中的隐射关系*/
	if(next_page_no != FIL_NULL){
		next_page = btr_page_get(space, next_page_no, RW_X_LATCH, mtr);
		btr_page_set_prev(next_page, upper_page_no, mtr);
	}

	/*修改lower page的前后关系*/
	btr_page_set_prev(lower_page, prev_page_no, mtr);
	btr_page_set_next(lower_page, upper_page_no, mtr);
	btr_page_set_level(lower_page, level, mtr);
	/*修改page的前后关系*/
	btr_page_set_prev(upper_page, lower_page_no, mtr);
	btr_page_set_next(upper_page, next_page_no, mtr);
	btr_page_set_level(upper_page, level, mtr);
}

rec_t* btr_page_split_and_insert(btr_cur_t* cursor, dtuple_t* tuple, mtr_t* mtr)
{
	dict_tree_t*	tree;
	page_t*		page;
	ulint		page_no;
	byte		direction;
	ulint		hint_page_no;
	page_t*		new_page;
	rec_t*		split_rec;
	page_t*		left_page;
	page_t*		right_page;
	page_t*		insert_page;
	page_cur_t*	page_cursor;
	rec_t*		first_rec;
	byte*		buf;
	rec_t*		move_limit;
	ibool		insert_will_fit;
	ulint		n_iterations = 0;
	rec_t*		rec;

func_start:
	tree = btr_cur_get_tree(cursor);

	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));
	ut_ad(rw_lock_own(dict_tree_get_lock(tree), RW_LOCK_EX));

	page = btr_cur_get_page(cursor);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));
	ut_ad(page_get_n_recs(page) >= 2);

	page_no = buf_frame_get_page_no(page);

	/*如果split_rec == NULL,意味着tuple是插入到upper page的第一条记录上,half-page*/
	if(n_iterations > 0){
		direction = FSP_UP;
		hint_page_no = page_no + 1;
		/*确定分裂的位置*/
		split_rec = btr_page_get_sure_split_rec(cursor, tuple);
	}
	else if(btr_page_get_split_rec_to_right(cursor, &split_rec)){ /*向右聚集分裂*/
		direction = FSP_UP;
		hint_page_no = page_no + 1;
	}
	else if(btr_page_get_split_rec_to_left(cursor, &split_rec)){ /*向左聚集分裂*/
		direction = FSP_DOWN;
		hint_page_no = page_no - 1;
	}
	else{
		direction = FSP_UP;
		hint_page_no = page_no + 1; /*表空间申请页的起始页号,作为分配页的一个预测依据*/
		/*从中间分裂,预测中间记录作为分裂点*/
		split_rec = page_get_middle_rec(page);
	}

	/*分配一个新page空间并初始化page*/
	new_page = btr_page_alloc(tree, hint_page_no, direction, btr_page_get_level(page, mtr), mtr);
	btr_page_create(new_page, tree, mtr);

	/*确定高位page的第一条记录*/
	if(split_rec != NULL){
		first_rec = split_rec;
		move_limit = split_rec;
	}
	else{
		buf = mem_alloc(rec_get_converted_size(tuple));
		/*将tuple转化成upper page的第一条记录*/
		first_rec = rec_convert_dtuple_to_rec(buf, tuple);
		move_limit = page_rec_get_next(btr_cur_get_rec(cursor));
	}

	/*进行page关联关系的修改*/
	btr_attach_half_pages(tree, page, first_rec, new_page, direction, mtr);
	if(split_rec == NULL)
		mem_free(buf);

	/*再次确定是否可以在split rec上插入tuple触发分裂,确定插入是否适合*/
	insert_will_fit = btr_page_insert_fits(cursor, split_rec, tuple);
	if(insert_will_fit && (btr_page_get_level(page, mtr) == 0)){ /*leaf page层*/
		mtr_memo_release(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK);
	}

	/*记录和行锁的转移*/
	if(direction == FSP_DOWN){ /*记录转移,向左聚集*/
		page_move_rec_list_start(new_page, page, move_limit, mtr);
		left_page = new_page;
		right_page = page;
		/*事务锁的继承和转移*/
		lock_update_split_left(right_page, left_page);
	}
	else{ /*向右聚集分裂*/
		page_move_rec_list_end(new_page, page, move_limit, mtr);
		left_page = page;
		right_page = new_page;

		lock_update_split_right(right_page, left_page);
	}

	/*确定新记录插入的页,因为页分裂了*/
	if(split_rec == NULL)
		insert_page = right_page;
	else if(cmp_tuple_rec(tuple, first_rec) >= 0)
		insert_page = right_page;
	else
		insert_page = left_page;

	/*进行tuple插入*/
	page_cursor = btr_cur_get_page_cur(cursor);
	page_cur_search(insert_page, tuple, PAGE_CUR_LE, page_cursor);
	rec = page_cur_tuple_insert(page_cursor, tuple, mtr);
	if(rec != NULL){
		ibuf_update_free_bits_for_two_pages_low(cursor->index, left_page, right_page, mtr);
		return rec;
	}

	/*如果tuple插入是不适合的，进行reorganization*/
	btr_page_reorganize(insert_page, mtr);
	page_cur_search(insert_page, tuple, PAGE_CUR_LE, page_cursor);
	rec = page_cur_tuple_insert(page_cursor, tuple, mtr);
	if(rec == NULL){ /*重组后再次尝试插入，还是不适合，可能要进行再次分裂*/
		ibuf_reset_free_bits(cursor->index, new_page);
		n_iterations++;
		ut_ad(n_iterations < 2);
		ut_ad(!insert_will_fit);

		goto func_start;
	}

	ibuf_update_free_bits_for_two_pages_low(cursor->index, left_page, right_page, mtr);

	ut_ad(page_validate(left_page, UT_LIST_GET_FIRST(tree->tree_indexes)));
	ut_ad(page_validate(right_page, UT_LIST_GET_FIRST(tree->tree_indexes)));

	return rec;
}

/*解除page的前后页关联关系*/
static void btr_level_list_remove(dict_tree_t* tree, page_t* page, mtr_t* mtr)
{
	ulint	space;
	ulint	prev_page_no;
	page_t*	prev_page;
	ulint	next_page_no;
	page_t*	next_page;

	ut_ad(tree && page && mtr);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	prev_page_no = btr_page_get_prev(page, mtr);
	next_page_no = btr_page_get_next(page, mtr);
	space = buf_frame_get_space_id(page);

	/*更改prev page的关联关系*/
	if(prev_page_no != FIL_NULL){
		prev_page = btr_page_get(space, prev_page_no, RW_X_LATCH, mtr);
		btr_page_set_next(prev_page, next_page_no, mtr);
	}
	/*更改next page的关联关系*/
	if(next_page_no != FIL_NULL){
		prev_page = btr_page_get(space, prev_page_no, RW_X_LATCH, mtr);
		btr_page_set_next(prev_page, next_page_no, mtr);
	}
}

/*写入一个记录偏移位置到redo log中*/
UNIV_INLINE void btr_set_min_rec_mark_log(rec_t* rec, mtr_t* mtr)
{
	mlog_write_initial_log_record(rec, MLOG_REC_MIN_MARK, mtr);
	/*写入一个记录想对page的偏移*/
	mlog_catenate_ulint(mtr, rec - buf_frame_align(rec), MLOG_2BYTES);
}

/*解析一个MLOG_REC_MIN_MARK类型的redo log*/
byte* btr_parse_set_min_rec_mark(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	rec_t* rec;
	if(end_ptr < ptr + 2)
		return NULL;

	if(page){
		rec = page + mach_read_from_2(ptr);
		btr_set_min_rec_mark(rec, mtr);
	}

	return ptr + 2;
}

/*设置一条minimum记录标识*/
void btr_set_min_rec_mark(rec_t* rec, mtr_t* mtr)
{
	ulint	info_bits;

	info_bits = rec_get_info_bits(rec);

	rec_set_info_bits(rec, info_bits | REC_INFO_MIN_REC_FLAG);

	btr_set_min_rec_mark_log(rec, mtr);
}

/*删除一个page的node ptr记录*/
void btr_node_ptr_delete(dict_tree_t* tree, page_t* page, mtr_t* mtr)
{
	rec_t*		node_ptr;
	btr_cur_t	cursor;
	ibool		compressed;
	ulint		err;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	node_ptr = btr_page_get_father_node_ptr(tree, page, mtr);

	btr_cur_position(UT_LIST_GET_FIRST(tree->tree_indexes), node_ptr, &cursor);

	compressed = btr_cur_pessimistic_delete(&err, TRUE, &cursor, FALSE, mtr);
	
	ut_a(err == DB_SUCCESS);
	if(!compressed)
		btr_cur_compress_if_useful(&cursor, mtr);
}

/*将page中的记录全部合并到father page上，减少btree 的层高*/
static void btr_lift_page_up(dict_tree_t* tree, page_t* page, mtr_t* mtr)
{
	rec_t*	node_ptr;
	page_t*	father_page;
	ulint	page_level;

	ut_ad(btr_page_get_prev(page, mtr) == FIL_NULL);
	ut_ad(btr_page_get_next(page, mtr) == FIL_NULL);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	/*获得page的父亲节点对应的PAGE*/
	node_ptr = btr_page_get_father_node_ptr(tree, page, mtr);
	father_page = buf_frame_algin(node_ptr);

	page_level = btr_page_get_level(page, mtr);
	/*取消page的hash索引*/
	btr_search_drop_page_hash_index(page);

	/*清空父亲page*/
	btr_page_empty(father_page, mtr);
	/*记录转移*/
	page_copy_rec_list_end(father_page, page, page_get_infimum_rec(page), mtr);
	/*将行锁转移*/
	lock_update_copy_and_discard(father_page, page);

	btr_page_set_level(father_page, page_level, mtr);

	btr_page_free(tree, page, mtr);

	ibuf_reset_free_bits(UT_LIST_GET_FIRST(tree->tree_indexes), father_page);

	ut_ad(page_validate(father_page, UT_LIST_GET_FIRST(tree->tree_indexes)));
	ut_ad(btr_check_node_ptr(tree, father_page, mtr));
}

void btr_compress(btr_cur_t* cursor, mtr_t* mtr)
{
	dict_tree_t*	tree;
	ulint		space;
	ulint		left_page_no;
	ulint		right_page_no;
	page_t*		merge_page;
	page_t*		father_page;
	ibool		is_left;
	page_t*		page;
	rec_t*		orig_pred;
	rec_t*		orig_succ;
	rec_t*		node_ptr;
	ulint		data_size;
	ulint		n_recs;
	ulint		max_ins_size;
	ulint		max_ins_size_reorg;
	ulint		level;

	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	level = btr_page_get_level(page, mtr);
	space = dict_tree_get_space(tree);

	left_page_no = btr_page_get_prev(page, mtr);
	right_page_no = btr_page_get_next(page, mtr);

	node_ptr = btr_page_get_father_node_ptr(tree, page, mtr);
	father_page = buf_frame_align(node_ptr);

	if(left_page_no != FIL_NULL){
		is_left = TRUE;
		merge_page = btr_page_get(space, left_page_no, RW_X_LATCH, mtr);
	}
	else if(right_page_no != FIL_NULL){
		is_left = FALSE;
		merge_page = btr_page_get(space, right_page_no, RW_X_LATCH, mtr);
	}
	else{ /*这一层只有1个page,直接合并到上一层上*/
		btr_lift_page_up(tree, page, mtr);
		return ;
	}

	n_recs = page_get_n_recs(page);
	data_size = page_get_data_size(page);

	/*获得最大可插入数据的空间*/
	max_ins_size_reorg = page_get_max_insert_size_after_reorganize(merge_page, n_recs);
	if(data_size > max_ins_size_reorg) /*不能合并,merge page空间不够*/
		return ;

	ut_ad(page_validate(merge_page, cursor->index));

	max_ins_size = page_get_max_insert_size(merge_page, n_recs);
	if (data_size > max_ins_size) { /*必须进行重组才能合并,重组是删除了无用的列和记录*/
		btr_page_reorganize(merge_page, mtr);

		ut_ad(page_validate(merge_page, cursor->index));
		ut_ad(page_get_max_insert_size(merge_page, n_recs) == max_ins_size_reorg);
	}

	btr_search_drop_page_hash_index(page);

	btr_level_list_remove(tree, page, mtr);
	if(is_left)
		btr_node_ptr_delete(tree, page, mtr);
	else{
		/*实际上是right_page_no代替了page的位置*/
		btr_node_ptr_set_child_page_no(node_ptr, right_page_no, mtr); /*更改node ptr对应的page no*/
		btr_node_ptr_delete(tree, merge_page, mtr); /*删除merge page对应的node ptr记录*/
	}

	/*记录和行锁的合并*/
	if(is_left){
		orig_pred = page_rec_get_prev( page_get_supremum_rec(merge_page));
		page_copy_rec_list_start(merge_page, page, page_get_supremum_rec(page), mtr);

		lock_update_merge_left(merge_page, orig_pred, page);
	}
	else{
		orig_succ = page_rec_get_next( page_get_infimum_rec(merge_page));
		page_copy_rec_list_end(merge_page, page, page_get_infimum_rec(page), mtr);

		lock_update_merge_right(orig_succ, page);
	}

	ibuf_update_free_bits_if_full(cursor->index, merge_page, UNIV_PAGE_SIZE, ULINT_UNDEFINED);

	btr_page_free(tree, page, mtr);

	ut_ad(btr_check_node_ptr(tree, merge_page, mtr));
}

/*废弃一个page,将行锁作用到上一层节点上*/
static void btr_discard_only_page_on_level(dict_tree_t* tree, page_t* page, mtr_t* mtr)
{
	rec_t*	node_ptr;
	page_t*	father_page;
	ulint	page_level;

	ut_ad(btr_page_get_prev(page, mtr) == FIL_NULL);
	ut_ad(btr_page_get_next(page, mtr) == FIL_NULL);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	btr_search_drop_page_hash_index(page);

	node_ptr = btr_page_get_father_node_ptr(tree, page, mtr);
	father_page = buf_frame_align(node_ptr);

	page_level = btr_page_get_level(page, mtr);

	/*将page的行锁全部转移到father page上*/
	lock_update_discard(page_get_supremum_rec(father_page), page);

	btr_page_set_level(father_page, mtr);

	btr_page_free(tree, page, mtr);

	if(buf_frame_get_page_no(father_page) == dict_tree_get_page(tree)){ /*已经到root了*/
		btr_page_empty(father_page, mtr);
		ibuf_reset_free_bits(UT_LIST_GET_FIRST(tree->tree_indexes), father_page);
	}
	else{
		ut_ad(page_get_n_recs(father_page) == 1);
		btr_discard_only_page_on_level(tree, father_page, mtr);
	}
}

void btr_discard_page(btr_cur_t* cursor, mtr_t* mtr)
{
	dict_tree_t*	tree;
	ulint		space;
	ulint		left_page_no;
	ulint		right_page_no;
	page_t*		merge_page;
	ibool		is_left;
	page_t*		page;
	rec_t*		node_ptr;

	page = btr_cur_get_page(cursor);
	tree = btr_cur_get_tree(cursor);

	ut_ad(dict_tree_get_page(tree) != buf_frame_get_page_no(page));
	ut_ad(mtr_memo_contains(mtr, dict_tree_get_lock(tree), MTR_MEMO_X_LOCK));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	space = dict_tree_get_space(tree);

	left_page_no = btr_page_get_prev(page, mtr);
	right_page_no = btr_page_get_next(page, mtr);

	if(left_page_no != FIL_NULL){
		is_left = TRUE;
		merge_page = btr_page_get(space, left_page_no, RW_X_LATCH, mtr);
	}
	else if(right_page_no != FIL_NULL){
		is_left = FALSE;
		merge_page = btr_page_get(space, right_page_no, RW_X_LATCH, mtr);
	}
	else{/*只有一个页？直接回合并到root下*/
		btr_discard_only_page_on_level(tree, page, mtr);
		return;
	}

	btr_search_drop_page_hash_index(page);

	if(left_page_no == FIL_NULL && btr_page_get_level(page, mtr) > 0){
		node_ptr = page_rec_get_next(page_get_infimum_rec(merge_page));
		ut_ad(node_ptr != page_get_supremum_rec(merge_page));
		/*标识最小row key的记录页*/
		btr_set_min_rec_mark(node_ptr, mtr);
	}
	/*从btree上删除page*/
	btr_level_list_remove(tree, page, mtr);

	if(is_left)
		lock_update_discard(page_get_supremum_rec(merge_page), page);
	else
		lock_update_discard(page_rec_get_next(page_get_infimum_rec(merge_page)), page);

	btr_page_free(tree, page, mtr);

	ut_ad(btr_check_node_ptr(tree, merge_page, mtr));
}

void
btr_print_size(
/*===========*/
	dict_tree_t*	tree)	/* in: index tree */
{
	page_t*		root;
	fseg_header_t*	seg;
	mtr_t		mtr;

	if (tree->type & DICT_IBUF) {
		printf(
	"Sorry, cannot print info of an ibuf tree: use ibuf functions\n");

		return;
	}

	mtr_start(&mtr);
	
	root = btr_root_get(tree, &mtr);

	seg = root + PAGE_HEADER + PAGE_BTR_SEG_TOP;

	printf("INFO OF THE NON-LEAF PAGE SEGMENT\n");
	fseg_print(seg, &mtr);

	if (!(tree->type & DICT_UNIVERSAL)) {

		seg = root + PAGE_HEADER + PAGE_BTR_SEG_LEAF;

		printf("INFO OF THE LEAF PAGE SEGMENT\n");
		fseg_print(seg, &mtr);
	}

	mtr_commit(&mtr); 	
}

/****************************************************************
Prints recursively index tree pages. */
static
void
btr_print_recursive(
/*================*/
	dict_tree_t*	tree,	/* in: index tree */
	page_t*		page,	/* in: index page */
	ulint		width,	/* in: print this many entries from start
				and end */
	mtr_t*		mtr)	/* in: mtr */
{
	page_cur_t	cursor;
	ulint		n_recs;
	ulint		i	= 0;
	mtr_t		mtr2;
	rec_t*		node_ptr;
	page_t*		child;
	
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page),
							MTR_MEMO_PAGE_X_FIX));
	printf("NODE ON LEVEL %lu page number %lu\n",
		btr_page_get_level(page, mtr), buf_frame_get_page_no(page));
	
	page_print(page, width, width);
	
	n_recs = page_get_n_recs(page);
	
	page_cur_set_before_first(page, &cursor);
	page_cur_move_to_next(&cursor);

	while (!page_cur_is_after_last(&cursor)) {

		if (0 == btr_page_get_level(page, mtr)) {

			/* If this is the leaf level, do nothing */

		} else if ((i <= width) || (i >= n_recs - width)) {

			mtr_start(&mtr2);

			node_ptr = page_cur_get_rec(&cursor);

			child = btr_node_ptr_get_child(node_ptr, &mtr2);

			btr_print_recursive(tree, child, width, &mtr2);
			mtr_commit(&mtr2);
		}

		page_cur_move_to_next(&cursor);
		i++;
	}
}

/******************************************************************
Prints directories and other info of all nodes in the tree. */

void
btr_print_tree(
	dict_tree_t*	tree,	/* in: tree */
	ulint		width)	/* in: print this many entries from start
				and end */
{
	mtr_t	mtr;
	page_t*	root;

	printf("--------------------------\n");
	printf("INDEX TREE PRINT\n");

	mtr_start(&mtr);

	root = btr_root_get(tree, &mtr);

	btr_print_recursive(tree, root, width, &mtr);

	mtr_commit(&mtr);

	btr_validate_tree(tree);
}

/****************************************************************
Checks that the node pointer to a page is appropriate. */

ibool
btr_check_node_ptr(
	dict_tree_t*	tree,	/* in: index tree */
	page_t*		page,	/* in: index page */
	mtr_t*		mtr)	/* in: mtr */
{
	mem_heap_t*	heap;
	rec_t*		node_ptr;
	dtuple_t*	node_ptr_tuple;

	ut_ad(mtr_memo_contains(mtr, buf_block_align(page),
							MTR_MEMO_PAGE_X_FIX));
	if (dict_tree_get_page(tree) == buf_frame_get_page_no(page)) {

		return(TRUE);
	}

	node_ptr = btr_page_get_father_node_ptr(tree, page, mtr);
 
	if (btr_page_get_level(page, mtr) == 0) {

		return(TRUE);
	}
	
	heap = mem_heap_create(256);
		
	node_ptr_tuple = dict_tree_build_node_ptr(
				tree,
				page_rec_get_next(page_get_infimum_rec(page)),
				0, heap, btr_page_get_level(page, mtr));
				
	ut_a(cmp_dtuple_rec(node_ptr_tuple, node_ptr) == 0);

	mem_heap_free(heap);

	return(TRUE);
}

/****************************************************************
Checks the size and number of fields in a record based on the definition of
the index. */
static
ibool
btr_index_rec_validate(
/*====================*/
				/* out: TRUE if ok */
	rec_t*		rec,	/* in: index record */
	dict_index_t*	index)	/* in: index */
{
	dtype_t* type;
	byte*	data;
	ulint	len;
	ulint	n;
	ulint	i;
	char	err_buf[1000];
	
	n = dict_index_get_n_fields(index);

	if (rec_get_n_fields(rec) != n) {
		fprintf(stderr, "Record has %lu fields, should have %lu\n",
				rec_get_n_fields(rec), n);

		rec_sprintf(err_buf, 900, rec);
	  	fprintf(stderr, "InnoDB: record %s\n", err_buf);

		return(FALSE);
	}

	for (i = 0; i < n; i++) {
		data = rec_get_nth_field(rec, i, &len);

		type = dict_index_get_nth_type(index, i);
		
		if (len != UNIV_SQL_NULL && dtype_is_fixed_size(type)
		    && len != dtype_get_fixed_size(type)) {
			fprintf(stderr,
			"Record field %lu len is %lu, should be %lu\n",
				i, len, dtype_get_fixed_size(type));

			rec_sprintf(err_buf, 900, rec);
	  		fprintf(stderr, "InnoDB: record %s\n", err_buf);

			return(FALSE);
		}
	}

	return(TRUE);			
}

/****************************************************************
Checks the size and number of fields in records based on the definition of
the index. */
static ibool btr_index_page_validate(
/*====================*/
				/* out: TRUE if ok */
	page_t*		page,	/* in: index page */
	dict_index_t*	index)	/* in: index */
{
	rec_t*		rec;
	page_cur_t 	cur;
	ibool		ret	= TRUE;
	
	page_cur_set_before_first(page, &cur);
	page_cur_move_to_next(&cur);

	for (;;) {
		rec = (&cur)->rec;

		if (page_cur_is_after_last(&cur)) {
			break;
		}

		if (!btr_index_rec_validate(rec, index)) {

			ret = FALSE;
		}

		page_cur_move_to_next(&cur);
	}

	return(ret);	
}

/****************************************************************
Validates index tree level. */
static ibool btr_validate_level(
	dict_tree_t*	tree,	/* in: index tree */
	ulint		level)	/* in: level number */
{
	ulint		space;
	page_t*		page;
	page_t*		right_page;
	page_t*		father_page;
	page_t*		right_father_page;
	rec_t*		node_ptr;
	rec_t*		right_node_ptr;
	ulint		right_page_no;
	ulint		left_page_no;
	page_cur_t	cursor;
	mem_heap_t*	heap;
	dtuple_t*	node_ptr_tuple;
	ibool		ret	= TRUE;
	dict_index_t*	index;
	mtr_t		mtr;
	char		err_buf[1000];
	
	mtr_start(&mtr);

	mtr_x_lock(dict_tree_get_lock(tree), &mtr);
	
	page = btr_root_get(tree, &mtr);

	space = buf_frame_get_space_id(page);

	while (level != btr_page_get_level(page, &mtr)) {

		ut_a(btr_page_get_level(page, &mtr) > 0);

		page_cur_set_before_first(page, &cursor);
		page_cur_move_to_next(&cursor);

		node_ptr = page_cur_get_rec(&cursor);
		page = btr_node_ptr_get_child(node_ptr, &mtr);
	}

	index = UT_LIST_GET_FIRST(tree->tree_indexes);
	
	/* Now we are on the desired level */
loop:
	mtr_x_lock(dict_tree_get_lock(tree), &mtr);

	/* Check ordering etc. of records */

	if (!page_validate(page, index)) {
		fprintf(stderr, "Error in page %lu in index %s\n",
			buf_frame_get_page_no(page), index->name);

		ret = FALSE;
	}

	if (level == 0) {
		if (!btr_index_page_validate(page, index)) {
 			fprintf(stderr,
				"Error in page %lu in index %s, level %lu\n",
				buf_frame_get_page_no(page), index->name,
								level);
			ret = FALSE;
		}
	}
	
	ut_a(btr_page_get_level(page, &mtr) == level);

	right_page_no = btr_page_get_next(page, &mtr);
	left_page_no = btr_page_get_prev(page, &mtr);

	ut_a((page_get_n_recs(page) > 0)
	     || ((level == 0) &&
		  (buf_frame_get_page_no(page) == dict_tree_get_page(tree))));

	if (right_page_no != FIL_NULL) {

		right_page = btr_page_get(space, right_page_no, RW_X_LATCH,
									&mtr);
		if (cmp_rec_rec(page_rec_get_prev(page_get_supremum_rec(page)),
			page_rec_get_next(page_get_infimum_rec(right_page)),
			UT_LIST_GET_FIRST(tree->tree_indexes)) >= 0) {

 			fprintf(stderr,
			"InnoDB: Error on pages %lu and %lu in index %s\n",
				buf_frame_get_page_no(page),
				right_page_no,
				index->name);

			fprintf(stderr,
			"InnoDB: records in wrong order on adjacent pages\n");

			rec_sprintf(err_buf, 900,
				page_rec_get_prev(page_get_supremum_rec(page)));
	  		fprintf(stderr, "InnoDB: record %s\n", err_buf);

			rec_sprintf(err_buf, 900,
			page_rec_get_next(page_get_infimum_rec(right_page)));
	  		fprintf(stderr, "InnoDB: record %s\n", err_buf);

	  		ret = FALSE;
	  	}
	}
	
	if (level > 0 && left_page_no == FIL_NULL) {
		ut_a(REC_INFO_MIN_REC_FLAG & rec_get_info_bits(
			page_rec_get_next(page_get_infimum_rec(page))));
	}

	if (buf_frame_get_page_no(page) != dict_tree_get_page(tree)) {

		/* Check father node pointers */
	
		node_ptr = btr_page_get_father_node_ptr(tree, page, &mtr);

		if (btr_node_ptr_get_child_page_no(node_ptr) !=
						buf_frame_get_page_no(page)
		   || node_ptr != btr_page_get_father_for_rec(tree, page,
		   	page_rec_get_prev(page_get_supremum_rec(page)),
								&mtr)) {
 			fprintf(stderr,
			"InnoDB: Error on page %lu in index %s\n",
				buf_frame_get_page_no(page),
				index->name);

			fprintf(stderr,
			"InnoDB: node pointer to the page is wrong\n");

			rec_sprintf(err_buf, 900, node_ptr);
				
	  		fprintf(stderr, "InnoDB: node ptr %s\n", err_buf);

			fprintf(stderr,
				"InnoDB: node ptr child page n:o %lu\n",
				btr_node_ptr_get_child_page_no(node_ptr));

			rec_sprintf(err_buf, 900,
			 	btr_page_get_father_for_rec(tree, page,
		   	 	page_rec_get_prev(page_get_supremum_rec(page)),
					&mtr));

	  		fprintf(stderr, "InnoDB: record on page %s\n",
								err_buf);
		   	ret = FALSE;

		   	goto node_ptr_fails;
		}

		father_page = buf_frame_align(node_ptr);

		if (btr_page_get_level(page, &mtr) > 0) {
			heap = mem_heap_create(256);
		
			node_ptr_tuple = dict_tree_build_node_ptr(
					tree,
					page_rec_get_next(
						page_get_infimum_rec(page)),
						0, heap,
       					btr_page_get_level(page, &mtr));

			if (cmp_dtuple_rec(node_ptr_tuple, node_ptr) != 0) {

	 			fprintf(stderr,
				  "InnoDB: Error on page %lu in index %s\n",
					buf_frame_get_page_no(page),
					index->name);

	  			fprintf(stderr,
                	"InnoDB: Error: node ptrs differ on levels > 0\n");
							
				rec_sprintf(err_buf, 900, node_ptr);
				
	  			fprintf(stderr, "InnoDB: node ptr %s\n",
								err_buf);
				rec_sprintf(err_buf, 900,
				  page_rec_get_next(
					page_get_infimum_rec(page)));
				
	  			fprintf(stderr, "InnoDB: first rec %s\n",
								err_buf);
		   		ret = FALSE;
				mem_heap_free(heap);

		   		goto node_ptr_fails;
			}

			mem_heap_free(heap);
		}

		if (left_page_no == FIL_NULL) {
			ut_a(node_ptr == page_rec_get_next(
					page_get_infimum_rec(father_page)));
			ut_a(btr_page_get_prev(father_page, &mtr) == FIL_NULL);
		}

		if (right_page_no == FIL_NULL) {
			ut_a(node_ptr == page_rec_get_prev(
					page_get_supremum_rec(father_page)));
			ut_a(btr_page_get_next(father_page, &mtr) == FIL_NULL);
		}

		if (right_page_no != FIL_NULL) {

			right_node_ptr = btr_page_get_father_node_ptr(tree,
							right_page, &mtr);
			if (page_rec_get_next(node_ptr) !=
					page_get_supremum_rec(father_page)) {

				if (right_node_ptr !=
						page_rec_get_next(node_ptr)) {
					ret = FALSE;
					fprintf(stderr,
			"InnoDB: node pointer to the right page is wrong\n");

	 				fprintf(stderr,
				  "InnoDB: Error on page %lu in index %s\n",
					buf_frame_get_page_no(page),
					index->name);
				}
			} else {
				right_father_page = buf_frame_align(
							right_node_ptr);
							
				if (right_node_ptr != page_rec_get_next(
					   		page_get_infimum_rec(
							right_father_page))) {
					ret = FALSE;
					fprintf(stderr,
			"InnoDB: node pointer 2 to the right page is wrong\n");

	 				fprintf(stderr,
				  "InnoDB: Error on page %lu in index %s\n",
					buf_frame_get_page_no(page),
					index->name);
				}

				if (buf_frame_get_page_no(right_father_page)
				   != btr_page_get_next(father_page, &mtr)) {

					ret = FALSE;
					fprintf(stderr,
			"InnoDB: node pointer 3 to the right page is wrong\n");

	 				fprintf(stderr,
				  "InnoDB: Error on page %lu in index %s\n",
					buf_frame_get_page_no(page),
					index->name);
				}
			}					
		}
	}

node_ptr_fails:
	mtr_commit(&mtr);

	if (right_page_no != FIL_NULL) {
		mtr_start(&mtr);
	
		page = btr_page_get(space, right_page_no, RW_X_LATCH, &mtr);

		goto loop;
	}

	return(ret);
}

/******************************************************************
Checks the consistency of an index tree. */

ibool
btr_validate_tree(dict_tree_t*	tree)	/* in: tree */
{
	mtr_t	mtr;
	page_t*	root;
	ulint	i;
	ulint	n;

	mtr_start(&mtr);
	mtr_x_lock(dict_tree_get_lock(tree), &mtr);

	root = btr_root_get(tree, &mtr);
	n = btr_page_get_level(root, &mtr);

	for (i = 0; i <= n; i++) {
		
		if (!btr_validate_level(tree, n - i)) {

			mtr_commit(&mtr);

			return(FALSE);
		}
	}

	mtr_commit(&mtr);

	return(TRUE);
}

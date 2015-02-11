#include "ibuf0ibuf.h"

#include "buf0buf.h"
#include "buf0rea.h"
#include "fsp0fsp.h"
#include "trx0sys.h"
#include "fil0fil.h"
#include "thr0loc.h"
#include "rem0rec.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "btr0btr.h"
#include "sync0sync.h"
#include "dict0boot.h"
#include "fut0lst.h"
#include "lock0lock.h"
#include "log0recv.h"
#include "que0que.h"


/* Buffer pool size per the maximum insert buffer size */
#define IBUF_POOL_SIZE_PER_MAX_SIZE	2

ibuf_t* ibuf			= NULL;
ulint ibuf_rnd			= 986058871;
ulint ibuf_flush_count	= 0;

#define IBUF_COUNT_N_SPACES 10
#define IBUF_COUNT_N_PAGES	10000

ulint*	ibuf_counts[IBUF_COUNT_N_SPACES];

ibool ibuf_counts_inited = FALSE;

#define IBUF_BITMAP			PAGE_DATA;

#define IBUF_BITMAP_FREE		0
#define IBUF_BITMAP_BUFFERED	2
#define IBUF_BITMAP_IBUF		3

#define IBUF_BITS_PER_PAGE		4

/* The mutex used to block pessimistic inserts to ibuf trees */
mutex_t	ibuf_pessimistic_insert_mutex;
/*insert buffer结构保护latch*/
mutex_t ibuf_mutex;
/*insert buffer bitmap结构保护latch*/
mutex_t ibuf_bitmap_mutex;

#define IBUF_MERGE_AREA			8

#define IBUF_MERGE_THRESHOLD	4

#define IBUF_MAX_N_PAGES_MERGED IBUF_MERGE_AREA

#define IBUF_CONTRACT_ON_INSERT_NON_SYNC	0
#define IBUF_CONTRACT_ON_INSERT_SYNC		5
#define IBUF_CONTRACT_DO_NOT_INSERT			10

/*insert buffer结构合法性判断*/
static ibool ibuf_validate_low(void);

/*设置当前os thread的ibuf标识*/
UNIV_INLINE void ibuf_enter()
{
	ibool* ptr;
	ptr = thr_local_get_in_ibuf_field();
	ut_ad(*ptr == FALSE);
	*ptr = TRUE;
}

UNIV_INLINE void ibuf_exit()
{
	ibool* ptr;
	ptr = thr_local_get_in_ibuf_field();
	ut_ad(*ptr = TRUE);
	*ptr = FALSE;
}

/*判断当前os thread是否在操作ibuf*/
ibool ibuf_inside()
{
	return *thr_local_get_in_ibuf_field();
}

/*获得ibuf header page,并且对其加上rw_x_latch*/
static page_t* ibuf_header_page_get(ulint space, mtr_t* mtr)
{
	page_t* page;

	ut_ad(!ibuf_inside());
	/*获得ibuf头页*/
	page = buf_page_get(space, FSP_IBUF_HEADER_PAGE_NO, RW_X_LATCH, mtr);
	buf_page_dbg_add_level(page, SYNC_IBUF_HEADER);

	return page;
}

/*获得ibuf btree的root page,并且对其加上rw_x_latch*/
static page_t* ibuf_tree_root_get(ibuf_data_t* data, ulint space, mtr_t* mtr)
{
	page_t* page;
	ut_ad(ibuf_inside());

	mtr_x_lock(dict_tree_get_lock((data->index)->tree), mtr);

	page = buf_page_get(space, FSP_IBUF_TREE_ROOT_PAGE_NO, RW_X_LATCH, mtr);
	buf_page_dbg_add_level(page, SYNC_TREE_NODE);

	return page;
}

/*获得指定page的ibuf count*/
ulint ibuf_count_get(ulint space, ulint page_no)
{
	ut_ad(space < IBUF_COUNT_N_SPACES);
	ut_ad(page_no < IBUF_COUNT_N_PAGES);

	if(!ibuf_counts_inited)
		return 0;

	return *(ibuf_counts[space] + page_no);
}

/*设置指定page的ibuf count*/
static void ibuf_count_set(ulint space, ulint page_no, ulint val)
{
	ut_ad(space < IBUF_COUNT_N_SPACES);
	ut_ad(page_no < IBUF_COUNT_N_PAGES);
	ut_ad(val < UNIV_PAGE_SIZE);

	*(ibuf_counts[space] + page_no) = val;
}

/*创建一个ibuf 结构并初始化*/
void ibuf_init_at_db_start()
{
	ibuf = mem_alloc(sizeof(ibuf_t));

	/*ibuf最大占用的内存空间为缓冲池的1/2, max_size是指页数，不是字节数*/
	ibuf->max_size = buf_pool_get_curr_size() / (UNIV_PAGE_SIZE * IBUF_POOL_SIZE_PER_MAX_SIZE);
	ibuf->meter = IBUF_THRESHOLD + 1;

	UT_LIST_INIT(ibuf->data_list);
	ibuf->size = 0;

	/*对ibuf_counts的初始化*/
	{
		ulint	i, j;

		for (i = 0; i < IBUF_COUNT_N_SPACES; i++) {

			ibuf_counts[i] = mem_alloc(sizeof(ulint)
				* IBUF_COUNT_N_PAGES);
			for (j = 0; j < IBUF_COUNT_N_PAGES; j++) {
				ibuf_count_set(i, j, 0);
			}
		}
	}

	/*创建线程并发的latch对象*/
	mutex_create(&ibuf_pessimistic_insert_mutex);
	mutex_set_level(ibuf_pessimistic_insert_mutex, SYNC_IBUF_PESS_INSERT_MUTEX);

	mutex_create(&ibuf_mutex);
	mutex_set_level(&ibuf_mutex, SYNC_IBUF_MUTEX);

	mutex_create(&ibuf_bitmap_mutex);
	mutex_set_level(&ibuf_bitmap_mutex, SYNC_IBUF_BITMAP_MUTEX);

	/*初始化ibuf对应的表空间文件*/
	fil_ibuf_init_at_db_start();

	ibuf_counts_inited = TRUE;
}

/*更新ibuf data的空间大小信息，假设segment的大小没做改变的情况下计算*/
static void ibuf_data_sizes_update(ibuf_data_t* data, page_t* root, mtr_t* mtr)
{
	ulint old_size;

	ut_ad(mutex_own(&ibuf_mutex));
	
	old_size = data->size;

	/*获得root page头中的ibuf free list*/
	data->free_list_len = flst_get_len(root + PAGE_HEADER + PAGE_BTR_IBUF_FREE_LIST, mtr);
	/*获得ibuf btree的层高*/
	data->height = btr_page_get_level(root, mtr) + 1;
	/*获得data占用的页数*/
	data->size = data->seg_size - (1 + data->free_list_len);

	ut_ad(data->size < data->seg_size);
	/*判断整个ibuf btree上是否有用户记录*/
	if(page_get_n_recs(root) > 0)
		data->empty = TRUE;
	else
		data->empty = FALSE;

	ut_ad(ibuf->size + data->size >= old_size);
	/*更新ibuf size，意思就是加上data->size 与old_size之间的差值*/
	ibuf->size = ibuf->size + data->size - old_size;
}

/*创建一个单独基于ibuf的表空间，这个函数会在fil_ibuf_init_at_db_start之中调用*/
ibuf_data_t* ibuf_data_init_for_space(ulint space)
{
	ibuf_data_t*	data;
	page_t*			root;
	page_t*			header_page;
	mtr_t			mtr;
	char			buf[50];
	dict_table_t*	table;
	dict_index_t*	index;
	ulint			n_used;

	data = mem_alloc(sizeof(ibuf_data_t));
	data->space = space;

	mtr_start(&mtr);

	mutex_enter(&ibuf_mutex);
	mtr_x_lock(fil_space_get_latch(space), &mtr);

	/*获得ibuf头页*/
	header_page = ibuf_header_page_get(space, &mtr);
	/*计算ibuf segment之中有多少页被占用了*/
	fseg_n_reserved_pages(header_page + IBUF_HEADER + IBUF_TREE_SEG_HEADER, &n_used, &mtr);

	ibuf_enter();
	/*第一个page为header页，第二个page为insert bitmap*/
	ut_ad(n_used >= 2);

	data->seg_size = n_used;

	root = buf_page_get(space, FSP_IBUF_TREE_ROOT_PAGE_NO, RW_X_LATCH, &mtr);
	buf_page_dbg_add_level(root, SYNC_TREE_NODE);

	data->size = 0;
	data->n_inserts = 0;
	data->n_merges = 0;
	data->n_merged_recs = 0;

	/*更新ibuf->size*/
	ibuf_data_sizes_update(data, root, &mtr);

	mutex_exit(&ibuf_mutex);
	mtr_commit(&mtr);
	ibuf_exit();

	/*在表字典中建立一张对应的ibuf表*/
	sprintf(buf, "SYS_IBUF_TABLE_%lu", space);

	table = dict_mem_table_create(buf, space, 2);
	dict_mem_table_add_col(table, "PAGE_NO", DATA_BINARY, 0, 0, 0);
	dict_mem_table_add_col(table, "TYPES", DATA_BINARY, 0, 0, 0);

	table->id = ut_dulint_add(DICT_IBUF_ID_MIN, space);

	dict_table_add_to_cache(table);

	index = dict_mem_index_create(buf, "CLUST_IND", space, DICT_CLUSTERED | DICT_UNIVERSAL | DICT_IBUF, 2);

	dict_mem_index_add_field(index, "PAGE_NO", 0);
	dict_mem_index_add_field(index, "TYPES", 0);

	index->page_no = FSP_IBUF_TREE_ROOT_PAGE_NO;
	index->id = ut_dulint_add(DICT_IBUF_ID_MIN, space);

	dict_index_add_to_cache(table, index);

	data->index = dict_table_get_first_index(table);

	/*将ibuf_data加入到ibuf中*/
	mutex_enter(&ibuf_mutex);
	UT_LIST_ADD_LAST(data_list, ibuf->data_list, data);
	mutex_exit(&ibuf_mutex);

	return data;
}

/*初始化ibuf bitamp age*/
void ibuf_bitmap_page_init(page_t* page, mtr_t* mtr)
{
	ulint	bit_offset;
	ulint	byte_offset;
	ulint	i;

	bit_offset = XDES_DESCRIBED_PER_PAGE * IBUF_BITS_PER_PAGE;
	byte_offset = bit_offset / 8 + 1;

	/*初始化为0*/
	for(i = IBUF_BITMAP; i < IBUF_BITMAP + byte_offset; i ++)
		*(page + 1) = 0;

	/*写入一条初始化ibuf bitmap的mtr log*/
	mlog_write_initial_log_record(page, MLOG_IBUF_BITMAP_INIT, mtr);
}

/*对初始化bitmap mtr log的重演*/
byte* ibuf_parse_bitmap_init(byte* ptr, byte* end_ptr, page_t* page, mtr_t* mtr)
{
	ut_ad(ptr && end_ptr);

	if(page)
		ibuf_bitmap_page_init(page, mtr);

	return ptr;
}

/*从ibuf bitmap中获取指定页的bitmap信息（一个4bit的整形数）*/
UNIV_INLINE ulint ibuf_bitmap_page_get_bits(page_t* page, ulint page_no, ulint bit, mtr_t* mtr)
{
	ulint	byte_offset;
	ulint	bit_offset;
	ulint	map_byte;
	ulint	value;

	ut_ad(bit < IBUF_BITS_PER_PAGE);
	ut_ad(IBUF_BITS_PER_PAGE % 2 == 0);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	/*一个page在bit当中占4位*/
	bit_offset = (page_no % XDES_DESCRIBED_PER_PAGE) * IBUF_BITS_PER_PAGE + bit;

	byte_offset = bit_offset / 8;
	bit_offset = bit_offset % 8;

	ut_ad(byte_offset + IBUF_BITMAP < UNIV_PAGE_SIZE);

	map_type = mach_read_from_1(page + IBUF_BITMAP + byte_offset);
	value = ut_bit_get_nth(map_byte, bit_offset);

	/*如果是取page剩余空间的话，那么是取前面2位*/
	if(bit == IBUF_BITMAP_FREE){
		ut_ad(bit_offset + 1 < 0);
		value = (value << 1) + ut_bit_get_nth(map_byte, bit_offset + 1);
	}

	return value;
}

/*设置一个page的ibuf bitmap信息*/
static void ibuf_bitmap_page_set_bits(page_t* page, ulint page_no, ulint bit, ulint val, mtr_t* mtr)
{
	ulint	byte_offset;
	ulint	bit_offset;
	ulint	map_byte;

	ut_ad(bit < IBUF_BITS_PER_PAGE);
	ut_ad(IBUF_BITS_PER_PAGE % 2 == 0);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(page), MTR_MEMO_PAGE_X_FIX));

	/*计算page对应的bitmap位置*/
	bit_offset = (page_no % XDES_DESCRIBED_PER_PAGE) * IBUF_BITS_PER_PAGE + bit;
	byte_offset = bit_offset / 8;
	bit_offset = bit_offset % 8;

	ut_ad(byte_offset + IBUF_BITMAP < UNIV_PAGE_SIZE);

	map_byte = mach_read_from_1(page + IBUF_BITMAP + byte_offset);

	/*设置页的剩余空间*/
	if (bit == IBUF_BITMAP_FREE){
		ut_ad(bit_offset + 1 < 8);
		ut_ad(val <= 3);

		map_byte = ut_bit_set_nth(map_byte, bit_offset, val / 2);
		map_byte = ut_bit_set_nth(map_byte, bit_offset + 1, val % 2);
	} 
	else{
		ut_ad(val <= 1);
		map_byte = ut_bit_set_nth(map_byte, bit_offset, val);
	}

	/*记录一条设置page bitmap信息的mtr log*/
	mlog_write_ulint(page + IBUF_BITMAP + byte_offset, map_byte, MLOG_1BYTE, mtr);
}

/*计算page_no的ibuf bitmap信息所在的bitmap page的序号位置*/
UNIV_INLINE ulint ibuf_bitmap_page_no_calc(ulint page_no)
{
	return FSP_IBUF_BITMAP_OFFSET + XDES_DESCRIBED_PER_PAGE * (page_no / XDES_DESCRIBED_PER_PAGE);
}

/*通过space 和page no获得对应page的bitmap所在的bitmap page对象*/
static page_t* ibuf_bitmap_get_map_page(ulint space, ulint page_no, mtr_t* mtr)
{
	page_t*	page;

	page = buf_get_get(space, ibuf_bitmap_page_no_calc(page_no), RW_X_LATCH, mtr);
	buf_page_dbg_add_level(page, SYNC_IBUF_BITMAP);

	return page;
}

/*设置page对应的ibuf bitmap信息*/
UNIV_INLINE void ibuf_set_free_bits_low(ulint type, page_t* page, ulint val, mtr_t* mtr)
{
	page_t*	bitmap_page;

	/*是聚集索引*/
	if(type & DICT_CLUSTERED)
		return ;

	/*不是btree的叶子节点*/
	if (btr_page_get_level_low(page) != 0)
		return;

	bitmap_page = ibuf_bitmap_get_map_page(buf_frame_get_space_id(page), buf_frame_get_page_no(page), mtr);

	ibuf_bitmap_page_set_bits(bitmap_page, buf_frame_get_page_no(page), IBUF_BITMAP_FREE, val, mtr);
}


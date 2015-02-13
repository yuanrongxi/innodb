#include "buf0lru.h"
#include "page0page.h"

extern ulint		ibuf_flush_count;

/*评估page剩余空间的计算参数，以1/32个页大小为单位*/
#define IBUF_PAGE_SIZE_PER_FREE_SPACE 32

/*insert bufffer存盘阈值,和ibuf_t->meter相关*/
#define IBUF_THRESHOLD 50

struct ibuf_data_struct
{
	ulint			space;		/*对应记录的table space id*/
	ulint			seg_size;	/*ibuf_data管理的page数量,表空间分配给ibuf_data的page数量*/
	ulint			size;		/*ibuf btree占用的page数量*/
	ibool			empty;		/*无任何缓冲记录标识*/
	ulint			free_list_len; /*ibuf_data可利用的空闲page*/
	ulint			height;		/*ibuf btree高度*/
	dict_index_t*	index;		/*ibuf 索引对象*/
	UT_LIST_NODE_T(ibuf_data_t) data_list;

	ulint			n_inserts;	/*插入到ibuf_data的次数*/
	ulint			n_merges;	/*ibuf被merge的次数*/
	ulint			n_merged_recs; /*从ibuf中被合并到辅助索引页中的记录总数*/
};

struct ibuf_struct
{
	ulint			size;		/*当前ibuf占用page空间数*/
	ulint			max_size;	/*最大可以占用的内存空间数*/
	ulint			meter;
	UT_LIST_BASE_NODE_T(ibuf_data_t) data_list;
};

void				ibuf_set_free_bits(ulint type, page_t* page, ulint val, ulint max_val);

/*假如一个insert能插入到ibuf当中，会对LRU做一次buf_LRU_try_free_flushed_blocks*/
UNIV_INLINE ibool	ibuf_should_try(dict_index_t* index, ulint ignore_sec_unique)
{
	if(!(index->type & DICT_CLUSTERED) && (ignore_sec_unique || !(index->type & DICT_UNIQUE)) && ibuf->meter > IBUF_THRESHOLD){
		ibuf_flush_count ++;
		if(ibuf_flush_count % 8 == 0) /*尝试将LRU中已经存盘的blocks进行释放到free list当中*/
			buf_LRU_try_free_flushed_blocks();

		return TRUE;
	}

	return FALSE;
}

/*检查page_no对应的page是否是ibuf bitmap page*/
UNIV_INLINE ibool  ibuf_bitmap_page(ulint page_no)
{
	/*bitmap page总insert buffer bitmap中的第二页, 一个insert buffer bitmap能管理16K个page*/
	if(page % XDES_DESCRIBED_PER_PAGE == FSP_IBUF_BITMAP_OFFSET)
		return TRUE;

	return FALSE;
}

/*Translates the free space on a page to a value in the ibuf bitmap.将空余空间转化为IBUF_BITMAP_FREE上的值:
 0	-	剩余页空间小于512B,也有可能没有任何空间
 1	-	剩余空间大于1/32 page_size
 2	-	剩余空间大于1/16 page_size
 3  -	剩余空间大于1/8 page_size*/
UNIV_INLINE ulint ibuf_index_page_calc_free_bits(ulint max_ins_size)
{
	ulint n;
	
	n = max_ins_size / (UNIV_PAGE_SIZE / IBUF_PAGE_SIZE_PER_FREE_SPACE);
	if(n == 3)
		n = 2;

	if(n > 3)
		n = 3;

	return n;
}

/*Translates the ibuf free bits to the free space on a page in bytes. */
UNIV_INLINE ulint ibuf_index_page_calc_free_from_bits(ulint bits)
{
	ut_ad(bits < 4);

	if(bit == 3)
		return (4 * UNIV_PAGE_SIZE / IBUF_PAGE_SIZE_PER_FREE_SPACE);

	return bits * UNIV_PAGE_SIZE / IBUF_PAGE_SIZE_PER_FREE_SPACE;
}

/*将page的记录重组后，剩余可用的空间用ibuf bitmap表示*/
UNIV_INLINE ulint ibuf_index_page_calc_free(page_t* page)
{
	return ibuf_index_page_calc_free_bits(page_get_max_insert_size_after_reorganize(page, 1));
}

/****************************************************************************
Updates the free bits of the page in the ibuf bitmap if there is not enough
free on the page any more. This is done in a separate mini-transaction, hence
this operation does not restrict further work to only ibuf bitmap operations,
which would result if the latch to the bitmap page were kept. */
UNIV_INLINE void ibuf_update_free_bits_if_full(dict_index_t* index, page_t* page,
	ulint max_ins_size, ulint increase)
{
	ulint before;
	ulint after;

	before = ibuf_index_page_calc_free_bits(max_ins_size);

	if(max_ins_size >= increase){
		ut_ad(ULINT_UNDEFINED > UNIV_PAGE_SIZE);
		after = ibuf_index_page_calc_free_bits(max_ins_size - increase);
	}
	else /*最后一次操作需要的空间大于max_ins_size,计算page重组后的页可用空间*/
		after = ibuf_index_page_calc_free(page);

	if(after == 0)
		buf_page_make_young(page);

	if(before > after)
		ibuf_set_free_bits(index->type, page, after, before);
}


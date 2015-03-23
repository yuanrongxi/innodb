#include "fut0lst.h"
#include "buf0buf.h"

static void flst_add_to_empty(flst_base_node_t* base, flst_node_t* node, mtr_t* mtr)
{
	ulint		space;
	fil_addr_t	node_addr;
	ulint		len;

	ut_ad(mtr && base && node);
	ut_ad(base != node);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node), MTR_MEMO_PAGE_X_FIX));

	len = flst_get_len(base, mtr);
	ut_a(len == 0);

	/*获得node对应的space和node_addr*/
	buf_ptr_get_fsp_addr(node, &space, &node_addr);

	/*写到头尾*/
	flst_write_addr(base + FLST_FIRST, node_addr, mtr);
	flst_write_addr(base + FLST_LAST, node_addr, mtr);

	/*写前后关系*/
	flst_write_addr(node + FLST_PREV, fil_addr_null, mtr);
	flst_write_addr(node + FLST_NEXT, fil_addr_null, mtr);

	/*增加单元格数*/
	mlog_write_ulint(base + FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

void flst_add_last(flst_base_node_t* base, flst_node_t* node, mtr_t* mtr)
{
	ulint			space;
	fil_addr_t		node_addr;
	ulint			len;
	fil_addr_t		last_addr;
	flst_node_t*	last_node;

	ut_ad(mtr && base && node);
	ut_ad(base != node);

	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node), MTR_MEMO_PAGE_X_FIX));

	len = flst_get_len(base, mtr);
	last_addr = flst_get_last(base, mtr);

	buf_ptr_get_fsp_addr(node, &space, &node_addr);

	if(len != 0){
		/*计算fut list的last node*/
		if(last_addr.page == node_addr.page)
			last_node = buf_frame_align(node) + last_addr.boffset;
		else
			last_node = fut_get_ptr(space, last_addr, RW_X_LATCH, mtr);

		/*将node插入到最后*/
		flst_insert_after(base, last_node, node, mtr);
	}
	else
		flst_add_to_empty(base, node, mtr); 
}

void flst_add_first(flst_base_node_t* base, flst_node_t* node, mtr_t* mtr)
{
	ulint			space;
	fil_addr_t		node_addr;
	ulint			len;
	fil_addr_t		first_addr;
	flst_node_t*	first_node;

	ut_ad(mtr && base && node);
	ut_ad(base != node);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node), MTR_MEMO_PAGE_X_FIX));

	len = flst_get_len(base, mtr);
	first_addr = flst_get_first(base, mtr);

	buf_ptr_get_fsp_addr(node, &space, &node_addr);

	if(len != 0){
		if(first_addr.page == node_addr.page)
			first_node = buf_frame_align(node) + first_addr.boffset;
		else
			first_node = fut_get_ptr(space, first_addr, RW_X_LATCH, mtr); /*对first_addr进行latch权限获得*/

		flst_insert_before(base, node, first_node, mtr);
	}
	else
		flst_add_to_empty(base, node, mtr); 
}

void flst_insert_after(flst_base_node_t*  base, flst_node_t* node1, flst_node_t* node2, mtr_t* mtr)
{
	ulint		space;
	fil_addr_t	node1_addr;
	fil_addr_t	node2_addr;
	flst_node_t*	node3;
	fil_addr_t	node3_addr;
	ulint		len;

	ut_ad(mtr && node1 && node2 && base);
	ut_ad(base != node1);
	ut_ad(base != node2);
	ut_ad(node2 != node1);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node1), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node2), MTR_MEMO_PAGE_X_FIX));

	buf_ptr_get_fsp_addr(node1, &space, &node1_addr);
	buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

	/*或者插入节点之前的node*/
	node3_addr = flst_get_next_addr(node1, mtr);

	/*建立前后关系*/
	flst_write_addr(node2 + FLST_PREV, node1_addr, mtr);
	flst_write_addr(node2 + FLST_NEXT, node3_addr, mtr);

	/*修改后一个节点的关联关系*/
	if(!fil_addr_is_null(node3_addr)){
		node3 = fut_get_ptr(space, node3_addr, RW_X_LATCH, mtr);
		flst_write_addr(node3 + FLST_PREV, node2_addr, mtr);
	}
	else /*直接插入到了最后*/
		flst_write_addr(base + FLST_LAST, node2_addr, mtr);

	/*修改前一个节点的关联关系*/
	flst_write_addr(node1 + FLST_NEXT, node2_addr, mtr);

	len = flst_get_len(base, mtr);
	mlog_write_ulint(base + FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

void flst_insert_before(flst_base_node_t* base, flst_node_t* node2, flst_node_t* node3, mtr_t* mtr)
{
	ulint			space;
	flst_node_t*	node1;
	fil_addr_t		node1_addr;
	fil_addr_t		node2_addr;
	fil_addr_t		node3_addr;
	ulint			len;

	ut_ad(mtr && node2 && node3 && base);
	ut_ad(base != node2);
	ut_ad(base != node3);
	ut_ad(node2 != node3);

	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node2), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node3), MTR_MEMO_PAGE_X_FIX));

	/*获得node2和node3的指针位置*/
	buf_ptr_get_fsp_addr(node2, &space, &node2_addr);
	buf_ptr_get_fsp_addr(node3, &space, &node3_addr);

	node1_addr = flst_get_prev_addr(node3, mtr);

	flst_write_addr(node2 + FLST_PREV, node1_addr, mtr);
	flst_write_addr(node2 _ FLST_NEXT, node3_addr, mtr);

	/*更改前面节点的对应关系*/
	if (!fil_addr_is_null(node1_addr)) {
		node1 = fut_get_ptr(space, node1_addr, RW_X_LATCH, mtr);
		flst_write_addr(node1 + FLST_NEXT, node2_addr, mtr);
	} 
	else
		flst_write_addr(base + FLST_FIRST, node2_addr, mtr);

	flst_write_addr(node3 + FLST_PREV, node2_addr, mtr);

	len = flst_get_len(base, mtr);
	mlog_write_ulint(base+ FLST_LEN, len + 1, MLOG_4BYTES, mtr);
}

void flst_remove(flst_base_node_t* base, flst_node_t* node2, mtr_t* mtr)
{
	ulint			space;
	flst_node_t*	node1;
	fil_addr_t		node1_addr;
	fil_addr_t		node2_addr;
	flst_node_t*	node3;
	fil_addr_t		node3_addr;
	ulint			len;

	ut_ad(mtr && node2 && base);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node2), MTR_MEMO_PAGE_X_FIX));

	buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

	node1_addr = flst_get_prev_addr(node2, mtr);
	node3_addr = flst_get_next_addr(node2, mtr);

	/*解除前面节点的对应关系*/
	if(!fil_addr_is_null(node1_addr)){
		if (node1_addr.page == node2_addr.page)
			node1 = buf_frame_align(node2) + node1_addr.boffset;
		else
			node1 = fut_get_ptr(space, node1_addr, RW_X_LATCH, mtr);

		ut_ad(node1 != node2);
		flst_write_addr(node1 + FLST_NEXT, node3_addr, mtr);
	}
	else
		flst_write_addr(base + FLST_NEXT, node3_addr, mtr);

	/*解除后面节点的对应关系*/
	if(!fil_addr_is_null(node3_addr)){
		if(node3_addr.page == node2_addr.page)
			node3 = buf_frame_align(node2) + node3_addr.boffset;
		else
			node3 = fut_get_ptr(space, node3_addr, RW_X_LATCH, mtr);

		ut_ad(node2 != node3);
		flst_write_addr(node3 + FLST_PREV, node1_addr, mtr);
	}
	else
		flst_write_addr(base + FLST_LAST, node1_addr, mtr);

	/*修改长度*/
	len = flst_get_len(base, mtr);
	ut_ad(len > 0);

	mlog_write_ulint(base + FLST_LEN, len - 1, MLOG_4BYTES, mtr); 
}

/*删除掉从node2之后的所有节点(包括node2)*/
void flst_cut_end(flst_base_node_t* base, flst_node_t* node2, ulint n_nodes, mtr_t* mtr)
{
	ulint			space;
	flst_node_t*	node1;
	fil_addr_t		node1_addr;
	fil_addr_t		node2_addr;
	ulint			len;

	ut_ad(mtr && node2 && base);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node2), MTR_MEMO_PAGE_X_FIX));
	ut_ad(n_nodes > 0);

	buf_ptr_get_fsp_addr(node2, &space, mtr);

	node1_addr = flst_get_prev_addr(node2, mtr);
	if(!fil_addr_is_null(node1_addr)){
		if(node1_addr.page == node2_addr.boffset)
			node1 = buf_frame_align(node2) + node1_addr.boffset;
		else
			node1 = fut_get_ptr(space, node1_addr, RW_X_LATCH, mtr);

		flst_write_addr(node1 + FLST_NEXT, fil_addr_null, mtr);
	}
	else
		flst_write_addr(base + FLST_FIRST, fil_addr_null, mtr);

	/*将node1置为最后的节点*/
	flst_write_addr(base + FLST_LAST, node1_addr, mtr);


	len = flst_get_len(base, mtr);
	ut_ad(len >= n_nodes);
	mlog_write_ulint(base + FLST_LEN, len - n_nodes, MLOG_4BYTES, mtr);
}

/*删除掉从node2之后的所有节点(不包括node2)*/
void flst_truncate_end(flst_base_node_t* base, flst_node_t* node2, ulint n_nodes, mtr_t* mtr)
{
	fil_addr_t	node2_addr;
	ulint		len;
	ulint		space;

	ut_ad(mtr && node2 && base);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));
	ut_ad(mtr_memo_contains(mtr, buf_block_align(node2), MTR_MEMO_PAGE_X_FIX));

	if(n_nodes == 0){
		ut_ad(fil_addr_is_null(flst_get_next_addr(node2, mtr)));	
		return ;
	}

	buf_ptr_get_fsp_addr(node2, &space, &node2_addr);

	flst_write_addr(node2 + FLST_NEXT, fil_addr_null, mtr);
	flst_write_addr(base + FLST_LAST, node2_addr, mtr);

	len = flst_get_len(base, mtr);
	ut_ad(len >= n_nodes);

	mlog_write_ulint(base + FLST_LEN, len - n_nodes, MLOG_4BYTES, mtr); 
}

/*校验磁盘链表的合法性*/
ibool flst_validate(flst_base_node_t* base, mtr_t*	mtr1)
{
	ulint		space;
	flst_node_t*	node;
	fil_addr_t	node_addr;
	fil_addr_t	base_addr;
	ulint		len;
	ulint		i;
	mtr_t		mtr2;
	
	ut_ad(base);
	ut_ad(mtr_memo_contains(mtr1, buf_block_align(base),
							MTR_MEMO_PAGE_X_FIX));

	/* We use two mini-transaction handles: the first is used to
	lock the base node, and prevent other threads from modifying the
	list. The second is used to traverse the list. We cannot run the
	second mtr without committing it at times, because if the list
	is long, then the x-locked pages could fill the buffer resulting
	in a deadlock. */

	/* Find out the space id */
	buf_ptr_get_fsp_addr(base, &space, &base_addr);

	len = flst_get_len(base, mtr1);
	node_addr = flst_get_first(base, mtr1);

	for (i = 0; i < len; i++) {
		mtr_start(&mtr2);

		node = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr2);
		node_addr = flst_get_next_addr(node, &mtr2);

		mtr_commit(&mtr2); 
	}
	
	ut_a(fil_addr_is_null(node_addr));

	node_addr = flst_get_last(base, mtr1);

	for (i = 0; i < len; i++) {
		mtr_start(&mtr2);

		node = fut_get_ptr(space, node_addr, RW_X_LATCH, &mtr2);
		node_addr = flst_get_prev_addr(node, &mtr2);

		mtr_commit(&mtr2);
	}
	
	ut_a(fil_addr_is_null(node_addr));

	return(TRUE);
}


void flst_print(flst_base_node_t* base, mtr_t* mtr)	
{
	buf_frame_t*	frame;
	ulint			len;

	ut_ad(base && mtr);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));

	frame = buf_frame_align(base);

	len = flst_get_len(base, mtr);

	printf("FILE-BASED LIST:\n");
	printf("Base node in space %lu page %lu byte offset %lu; len %lu\n", buf_frame_get_space_id(frame), buf_frame_get_page_no(frame), (ulint) (base - frame), len);
}




#include "fut0fut.h"
#include "mtr0log.h"
#include "buf0buf.h"

#define FLST_PREV			0
#define FLST_NEXT			FIL_ADDR_SIZE
#define FLST_LEN			0
#define FLST_FIRST			4
#define FLST_LAST			(4 + FIL_ADDR_SIZE)

UNIV_INLINE void flst_write_addr(fil_faddr_t* faddr, fil_addr_t addr, mtr_t* mtr)
{
	ut_ad(faddr && mtr);
	ut_ad(mtr_memo_contains(mtr, buf_block_align(faddr), MTR_MEMO_PAGE_X_FIX));

	mlog_write_ulint(faddr + FIL_ADDR_PAGE, addr.page, MLOG_4BYTES, mtr);
	mlog_write_ulint(faddr + FIL_ADDR_BYTE, addr.boffset, MLOG_2BYTES, mtr);
}

UNIV_INLINE fil_addr_t flst_read_addr(fil_faddr_t* faddr, mtr_t* mtr)
{
	fil_addr_t	addr;

	ut_ad(faddr && mtr);
	addr.page = mtr_read_ulint(faddr + FIL_ADDR_PAGE, MLOG_4BYTES, mtr);
	addr.boffset = mtr_read_ulint(faddr + FIL_ADDR_BYTE, MLOG_2BYTES, mtr);

	return addr;
}

UNIV_INLINE void flst_init(flst_base_node_t* base, mtr_t* mtr)
{
	ut_ad(mtr_memo_contains(mtr, buf_block_align(base), MTR_MEMO_PAGE_X_FIX));

	mlog_write_ulint(base + FLST_LEN, 0, MLOG_4BYTES, mtr);
	flst_write_addr(base + FLST_FIRST, fil_addr_null, mtr);
	flst_write_addr(base + FLST_LAST, fil_addr_null, mtr);
}

UNIV_INLINE ulint flst_get_len(flst_base_node_t* base, mtr_t* mtr)
{
	return mtr_read_ulint(base + FLST_LEN, MLOG_4BYTES, mtr);
}

UNIV_INLINE fil_addr_t flst_get_first(flst_base_node_t* base, mtr_t* mtr)
{
	return flst_read_addr(base + FLST_FIRST, mtr);
}

UNIV_INLINE fil_addr_t flst_get_last(flst_base_node_t* base, mtr_t* mtr)
{
	return flst_read_addr(base + FLST_LAST, mtr);
}

UNIV_INLINE fil_addr_t flst_get_next_addr(flst_node_t* node, mtr_t* mtr)
{
	return flst_read_addr(node + FLST_NEXT, mtr);
}

UNIV_INLINE fil_addr_t flst_get_prev_addr(flst_node_t* node, mtr_t* mtr)
{
	return(flst_read_addr(node + FLST_PREV, mtr));
}





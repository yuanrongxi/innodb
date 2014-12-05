#include "sync0rw.h"
#include "buf0buf.h"

UNIV_INLINE btye* fut_get_ptr(ulint space, fil_addr_t addr, ulint rw_latch, mtr_t* mtr)
{
	byte* ptr;

	ut_ad(mtr);
	ut_ad(addr.boffset < UNIV_PAGE_SIZE);
	ut_ad((rw_latch == RW_S_LATCH) || (rw_latch == RW_X_LATCH));

	ptr = buf_page_get(space, addr.page, rw_latch, mtr) + addr.boffset;
	buf_page_dbg_add_level(ptr, SYNC_NO_ORDER_CHECK);

	return ptr;
}



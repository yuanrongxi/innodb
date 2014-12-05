#ifndef __FSP0FSP_H_
#define __FSP0FSP_H_

#include "univ.h"
#include "mtr0mtr.h"
#include "fut0lst.h"
#include "ut0byte.h"
#include "page0types.h"

#define FSP_UP		((byte)111)
#define FSP_DOWN	((byte)112)
#define FSP_NO_DIR	((byte)113)

#define FSP_EXTENT_SIZE		64

#define FSEG_PAGE_DATA		FIL_PAGE_DATA

/*定义segment的偏移*/
#define FSEG_HDR_SPACE		0	/*space id*/
#define FSEG_HDR_PAGE_NO	4	/*page number*/
#define FSEG_HDR_OFFSET		8	/*byte offset*/
#define FSEG_HEADER_SIZE	10

#define FSP_NORMAL			1000000
#define FSP_UNDO			2000000
#define FSP_CLEANING		3000000

#define XDES_DESCRIBED_PER_PAGE		UNIV_PAGE_SIZE

#define FSP_XDES_OFFSET				0
#define FSP_IBUF_BITMAP_OFFSET		1

#define FSP_FIRST_INODE_PAGE_NO		2
#define FSP_IBUF_HEADER_PAGE_NO		3
#define FSP_IBUF_TREE_ROOT_PAGE_NO	4

#define FSP_TRX_SYS_PAGE_NO			5
#define FSP_FIRST_PSEG_PAGE_NO		6
#define FSP_DICT_HDR_PAGE_NO		7

/*segment头*/
typedef byte fseg_header_t;



void					fsp_init();

ulint					fsp_header_get_free_limit(ulint space);

ulint					fsp_header_get_tablespace_size(ulint space);
/*增加space的size*/
void					fsp_header_inc_size(ulint space, ulint size_inc, mtr_t* mtr);
/*建立一个segment*/
page_t*					fseg_create(ulint space, ulint page, ulint byte_offset, mtr_t* mtr);
page_t*					fseg_create_general(ulint space, ulint page, ulint byte_offset, ibool has_done_reservation, mtr_t* mtr);

ulint					fseg_n_reserved_pages(fseg_header_t* header, ulint* used, mtr_t* mtr);

ulint					fseg_alloc_free_page(fseg_header_t* seg_header, ulint hint, byte direction, mtr_t* mtr);

ulint					fseg_alloc_free_page_general(fseg_header_t* seg_header, ulint hint, byte direction, ibool has_done_reservation, mtr_t* mtr);

ulint					fsp_reserve_free_extents(ulint space, ulint n_ext, ulint alloc_type, mtr_t* mtr);

ulint					fsp_get_available_space_in_free_extents(ulint space);

void					fseg_free_page(fseg_header_t* seg_header, ulint space, ulint page, mtr_t* mtr);

void					fseg_free(ulint space, ulint page_no, ulint offset);

ibool					fseg_free_step(fseg_header_t* header, mtr_t* mtr);

ibool					fseg_free_step_not_header(fseg_header_t* header, mtr_t* mtr);

UNIV_INLINE	ibool		fsp_descr_page(ulint page_no);

byte*					fsp_parse_init_file_page(byte* ptr, byte* end_ptr, page_t* page);

ibool					fsp_validate(ulint space);

void					fsp_print(ulint space);

ibool					fseg_validate(fseg_header_t* header, mtr_t* mtr1);

void					fsp_print(fseg_header_t* header, mtr_t* mtr);

#include "fspfsp.inl"


#endif







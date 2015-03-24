#ifndef __dict0boot_h_
#define __dict0boot_h_

/**************************************************************************
*对字典元数据的读取，存储在space = 0的系统表空间里
**************************************************************************/

#include "univ.h"
#include "mtr0mtr.h"
#include "mtr0log.h"
#include "ut0byte.h"
#include "buf0buf.h"
#include "fsp0fsp.h"
#include "dict0dict.h"

#define DICT_HDR_SPACE			0						/*系统表空间ID*/
#define DICT_HDR_PAGE_NO		FSP_DICT_HDR_PAGE_NO

/* The ids for the basic system tables and their indexes */
#define DICT_TABLES_ID			ut_dulint_create(0, 1)
#define DICT_COLUMNS_ID			ut_dulint_create(0, 2)
#define DICT_INDEXES_ID			ut_dulint_create(0, 3)
#define DICT_FIELDS_ID			ut_dulint_create(0, 4)
#define DICT_TABLE_IDS_ID		ut_dulint_create(0, 5)

#define DICT_HDR_FIRST_ID		10

#define DICT_IBUF_ID_MIN		ut_dulint_create(0xFFFFFFFF, 0)

#define DICT_HDR				FSEG_PAGE_DATA

/* Dictionary header offsets,字典头偏移*/
#define DICT_HDR_ROW_ID			0	/* The latest assigned row id */
#define	DICT_HDR_TABLE_ID		8	/* The latest assigned table id */
#define	DICT_HDR_INDEX_ID		16	/* The latest assigned index id */
#define	DICT_HDR_MIX_ID			24	/* The latest assigned mix id */
#define	DICT_HDR_TABLES			32	/* Root of the table index tree */
#define	DICT_HDR_TABLE_IDS		36	/* Root of the table index tree */
#define	DICT_HDR_COLUMNS		40	/* Root of the column index tree */
#define	DICT_HDR_INDEXES		44	/* Root of the index index tree */
#define	DICT_HDR_FIELDS			48	/* Root of the index field index tree */

#define DICT_HDR_FSEG_HEADER	56

/* The field number of the page number field in the sys_indexes table clustered index */
#define DICT_SYS_INDEXES_PAGE_NO_FIELD	 8
#define DICT_SYS_INDEXES_SPACE_NO_FIELD	 7

#define DICT_HDR_ROW_ID_WRITE_MARGIN	256

typedef byte					dict_hdr_t;

/*************************************函数**********************************/
dict_hdr_t*						dict_hdr_get(mtr_t* mtr);

dulint							dict_hdr_get_new_id(ulint type);

UNIV_INLINE dulint				dict_sys_get_new_row_id();

UNIV_INLINE dulint				dict_sys_read_row_id(byte* field);

UNIV_INLINE void				dict_sys_write_row_id(byte* field, dulint row_id);

void							dict_boot();

void							dict_create();

#endif




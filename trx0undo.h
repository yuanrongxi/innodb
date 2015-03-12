#ifndef __trx0undo_h_
#define __trx0undo_h_

#include "univ.h"
#include "trx0types.h"
#include "mtr0mtr.h"
#include "trx0sys.h"
#include "page0types.h"

struct trx_undo_struct
{
	ulint				id;				/*undo log segment在rollback segment的slot id*/
	ulint				type;			/*undo的类型：insert或者update*/
	ulint				state;
	ibool				del_marks;		/*删除表示，TRUE时事务进行了delete mark操作或者事务更新了extern属性列*/
	ulint				trx_id;			/*事务ID*/
	ibool				dict_operation;	/*是否是DDL操作*/
	dulint				table_id;		/*DDLD对应的table id*/
	trx_rseg_t*			rseg;			/*对应的回滚段句柄*/
	ulint				space;			/*undo log对应的表空间ID*/
	ulint				hdr_page_no;	/*undo log header所在的页序号*/
	ulint				hdr_offset;		/*undo log header所在页中的偏移量*/
	ulint				last_page_no;	/*undo log最后更新的的undo page页的序号*/
	ulint				size;			/*当前undo log占用的页数量*/
	ulint				empty;			/*undo log是否为空，如果事务全是select操作，这个标示会生效*/
	ulint				top_page_no;	/*最后一个undo log日志所在的page序号*/
	ulint				top_offset;		/*最后一个undo log日志所在的page页中的偏移*/
	dulint				top_undo_no;	/*最后一个undo log日志的undo no*/
	page_t*				guess_page;		/*最后一个undo log在buffer pool中的页指针*/
	UT_LIST_NODE_T(trx_undo_t) undo_list;
};

/*undo log page的结构定义*/
#define TRX_UNDO_PAGE_HDR				FSEG_PAGE_DATA
#define TRX_UNDO_PAGE_TYPE				0
#define TRX_UNDO_PAGE_START				2
#define TRX_UNDO_PAGE_FREE				4
#define TRX_UNDO_PAGE_NODE				6
#define TRX_UNDO_PAGE_HDR_SIZE			(6 + FLST_NODE_SIZE)
#define TRX_UNDO_PAGE_REUSE_LIMIT		(3 * UNIV_PAGE_SIZE / 4)

#define	TRX_UNDO_SEG_HDR				(TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE)
#define	TRX_UNDO_STATE					0
#define	TRX_UNDO_LAST_LOG				2
#define	TRX_UNDO_FSEG_HEADER			4
#define	TRX_UNDO_PAGE_LIST				(4 + FSEG_HEADER_SIZE)
#define TRX_UNDO_SEG_HDR_SIZE			(4 + FSEG_HEADER_SIZE + FLST_BASE_NODE_SIZE)
#define	TRX_UNDO_TRX_ID					0
#define TRX_UNDO_TRX_NO					8
#define TRX_UNDO_DEL_MARKS				16
#define	TRX_UNDO_LOG_START				18
#define	TRX_UNDO_DICT_OPERATION			20
#define TRX_UNDO_TABLE_ID				22
#define	TRX_UNDO_NEXT_LOG				30
#define TRX_UNDO_PREV_LOG				32
#define TRX_UNDO_HISTORY_NODE			34
#define TRX_UNDO_LOG_HDR_SIZE			(34 + FLST_NODE_SIZE)

/*undo log segment的类型*/
#define TRX_UNDO_INSERT					1
#define TRX_UNDO_UPDATE					2
/*undo log segment的状态*/
#define TRX_UNDO_ACTIVE					1
#define TRX_UNDO_CACHED					2
#define TRX_UNDO_TO_FREE				3
#define TRX_UNDO_TO_PURGE				4

UNIV_INLINE dulint trx_undo_build_roll_ptr(ibool is_insert, ulint rseg_id, ulint page_no, ulint offset);

#include "trx0undo.inl"

#endif





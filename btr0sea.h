/***********************************************************************
*
*自适应HASH索引
***********************************************************************/
#ifndef __btr0sea_h_
#define __btr0sea_h_

#include "univ.h"

#include "rem0rec.h"
#include "dict0dict.h"
#include "btr0types.h"
#include "mtr0mtr.h"
#include "ha0ha.h"
#include "page0types.h"

/* 搜索信息方向*/
#define BTR_SEA_NO_DIRECTION	1
#define BTR_SEA_LEFT			2		/**/
#define BTR_SEA_RIGHT			3
#define BTR_SEA_SAME_REC		4

/*hash node的魔法校验字*/
#define BTR_SEARCH_MAGIC_N		1112765

/*hash analysis的上限阈值*/
#define BTR_SEARCH_HASH_ANALYSIS 17

#define BTR_SEARCH_LEFT_SIDE	1
#define BTR_SEARCH_RIGHT_SIDE	2

#define BTR_SEARCH_ON_PATTERN_LIMIT	3
#define BTR_SEARCH_ON_HASH_LIMIT	3

#define BTR_SEA_TIMEOUT			10000

struct btr_search_struct
{
	ulint						magic_n;			/*btr_search magic*/
	rec_t*						last_search;
	ulint						n_direction;		/*同一方向连续search的个数*/
	ulint						direction;			/*TR_SEA_NO_DIRECTION, BTR_SEA_LEFT,BTR_SEA_RIGHT, BTR_SEA_SAME_REC,or BTR_SEA_SAME_PAGE*/
	dulint						modify_clock;		/*last_search最后更改时刻*/

	page_t*						root_guess;

	ulint						hash_analysis;
	ibool						last_hash_succ;
	ulint						n_hash_potential;	/*连续用自适应hash查找成功的个数*/
	ulint						n_fields;
	ulint						n_bypes;
	ulint						side;

	ulint						n_hash_succ;
	ulint						n_hash_fail;
	ulint						n_patt_succ;
	ulint						n_searches;
};

typedef struct btr_search_sys_struct
{
	hash_table_t*				hash_index;
}btr_search_sys_t;

/*自适应hash索引全局对象*/
extern btr_search_sys_t*		btr_search_sys;

/*互斥latch*/
extern rw_lock_t*				btr_search_latch_temp;
#define btr_search_latch		(*btr_search_latch_temp)
/*统计信息*/
extern ulint					btr_search_n_succ;
extern ulint					btr_search_n_hash_fail;

/***************************function***********************/

void							btr_search_sys_create(ulint hash_size);

UNIV_INLINE btr_search_t*		btr_search_get_info(dict_index_t* index);

btr_search_t*					btr_search_info_create(mem_heap_t* heap);

UNIV_INLINE void				btr_search_info_update(dict_index_t* index, btr_cur_t* cursor);

ibool							btr_search_guess_on_pattern(dict_index_t* index, btr_search_t* info, dtuple_t* tuple, ulint latch_mode,
										btr_cur_t* cursor, mtr_t* mtr);

ibool							btr_search_guess_on_hash(dict_index_t* index, btr_search_t* info, dtuple_t* tuple, ulint mode, ulint latch_mode,
										btr_cur_t* cursor, ulint has_search_latch, mtr_t* mtr);

void							btr_search_move_or_delete_hash_entries(page_t* new_page, page_t* page);

void							btr_search_drop_page_hash_index(page_t* page);

void							btr_search_drop_page_hash_when_freed(ulint space, ulint pge_no);

void							btr_search_update_hash_node_on_insert(btr_cur_t* cursor);

void							btr_search_update_hash_on_insert(btr_cur_t* cursor);

void							btr_search_update_hash_on_delete(btr_cur_t* cursor);

void							btr_search_print_info(void);

void							btr_search_index_print_info(dict_index_t* index);

void							btr_search_table_print_info(char* name);

ibool							btr_search_validate(void);

#include "btr0sea.inl"

#endif
/**********************************************************/





#include "dict0mem.h"
#include "btr0cur.h"
#include "buf0buf.h"

void btr_search_info_update_slow(btr_search_t* info, btr_cur_t* cursor);

/*获得index索引的search info*/
UNIV_INLINE btr_search_t* btr_search_get_info(dict_index_t* index)
{
	ut_ad(index);

	return index->search_info;
}

/*对index中的search info*/
UNIV_INLINE void btr_search_info_update(dict_index_t* index, btr_cur_t* cursor)
{
	btr_search_t* info;

	ut_ad(!rw_lock_own(&btr_search_latch, RW_LOCK_SHARED) && !rw_lock_own(&btr_search_latch, RW_LOCK_EX));

	info = btr_search_get_info(index);
	
	info->hash_analysis ++;
	if(info->hash_analysis < BTR_SEARCH_HASH_ANALYSIS) /*每17次做一次update?*/
		return;

	ut_ad(cursor->flag != BTR_CUR_HASH);

	btr_search_info_update_slow(info, cursor);
}



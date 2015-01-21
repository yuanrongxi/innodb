#include "sync0sync.h"
#include "srv0srv.h"
#include "dict0dict.h"
#include "row0row.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "buf0buf.h"
#include "page0page.h"
#include "page0cur.h"
#include "row0vers.h"
#include "que0que.h"
#include "btr0cur.h"
#include "read0read.h"
#include "log0recv.h"

UNIV_INLINE ulint lock_rec_fold(ulint space, ulint page_no)
{
	return ut_fold_ulint_pair(space, page_no);
}

UNIV_INLINE ulint lock_rec_hash(ulint space, ulint page_no)
{
	return hash_calc_hash(lock_rec_fold(space, page_no), lock_sys->rec_hash);
}

/*通过聚集索引获得记录行隐式x-lock对应的事务对象*/
UNIV_INLINE trx_t* lock_clust_rec_some_has_impl(rec_t* rec, dict_index_t* index)
{
	dulint	trx_id;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(page_rec_is_user_rec(rec));

	/*通过index和rec记录获得事务ID*/
	trx_id = row_get_rec_trx_id(rec, index);
	if(trx_is_active(trx_id)) /*假如事务ID为激活的事务的ID，则表示记录存在隐式索引*/
		return trx_get_on_id(trx_id);

	return NULL;
}




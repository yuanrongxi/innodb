#ifndef __btr0pcur_h_
#define __btr0pcur_h_

#include "univ.h"
#include "dict0dict.h"
#include "data0data.h"
#include "mtr0mtr.h"
#include "page0cur.h"
#include "btr0cur.h"
#include "btr0btr.h"
#include "btr0types.h"


#define BTR_PCUR_ON				1
#define BTR_PCUR_BEFORE			2
#define BTR_PCUR_AFTER			3

#define BTR_PCUR_BEFORE_FIRST_IN_TREE	4
#define BTR_PCUR_AFTER_LAST_IN_TREE		5

/*pcursor->pos_state value*/
#define BTR_PCUR_IS_POSITIONED			1997660512
#define BTR_PCUR_WAS_POSITIONED			1187549791
#define BTR_PCUR_NOT_POSITIONED			1328997689

/*pcursor->old_stored value*/
#define BTR_PCUR_OLD_STORED				908467085
#define BTR_PCUR_OLD_NOT_STORED			122766467

/*BTree persistent cursor的状态结构信息*/
struct btr_pcur_struct
{
	btr_cur_t			btr_cur;			/*btree游标*/
	ulint				latch_mode;			/*latch 模式，x-latch s-latch*/
	ulint				old_stored;			/*BTR_PCUR_OLD_STORED/BTR_PCUR_OLD_NOT_STORED,表示是否存有上一条记录的标识*/
	rec_t*				old_rec;			
	ulint				rel_pos;			/*BTR_PCUR_ON, BTR_PCUR_BEFORE, or BTR_PCUR_AFTER,old cursor页位置方向标识*/
	dulint				modify_clock;		/*store old rec时ibuf的modify clock*/
	ulint				pos_state;			/*BTR_PCUR_IS_POSITIONED..*/
	ulint				search_mode;		/*搜索方式：PAGE_CUR_G..*/
	mtr_t*				mtr;				/*mini trancation*/
	byte*				old_rec_buf;
	ulint				buf_size;
};


btr_pcur_t*				btr_pcur_create_for_mysql();
void					btr_pcur_free_for_mysql(btr_pcur_t* cursor);
void					btr_pcur_copy_stored_position(btr_pcur_t* pcur_receive, btr_pcur_t* pcur_donate);

UNIV_INLINE void		btr_pcur_init(dict_index_t* index, dtuple_t* tuple, ulint mode, ulint latch_mode, btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE void		btr_pcur_open_with_no_init(dict_index_t* index, dtuple_t* tuple, ulint mode, ulint latch_mode, btr_pcur_t* cursor, ulint has_search_latch, mtr_t* mtr);

UNIV_INLINE void		btr_pcur_open_at_index_side(ibool from_left, dict_index_t* index, ulint latch_mode, btr_pcur_t* pcur, ibool do_init, mtr_t* mtr);

UNIV_INLINE ulint		btr_pcur_get_up_match(btr_pcur_t* cursor);

UNIV_INLINE ulint		btr_pcur_get_low_match(btr_pcur_t* cursor);

void					btr_pcur_open_on_user_rec(dict_index_t* index, dtuple_t* tuple, ulint mode, ulint latch_mode, btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE	void		btr_pcur_open_at_rnd_pos(dict_index_t* index, ulint latch_mode, btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE	void		btr_pcur_close(btr_pcur_t* cursor);

void					btr_pcur_store_position(btr_pcur_t* cursor, mtr_t* mtr);

void					btr_pcur_restore_position(ulint latch_mode, btr_pcur_t* pcur, mtr_t* mtr);

void					btr_pcur_release_leaf(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE	ulint		btr_pcur_get_rel_pos(btr_pcur_t* cursor);

UNIV_INLINE void		btr_pcur_set_mtr(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE mtr_t*		btr_pcur_get_mtr(btr_pcur_t* cursor);

UNIV_INLINE void		btr_pcur_commit(btr_pcur_t* pcur);

UNIV_INLINE void		btr_pcur_commit_specify_mtr(btr_pcur_t* pcur, mtr_t* mtr);

UNIV_INLINE ibool		btr_pcur_is_detached(btr_pcur_t* pcur);

UNIV_INLINE ibool		btr_pcur_move_to_next(btr_pcur_t* cursor, mtr_t* mtr);

ibool					btr_pcur_move_to_prev(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE	ibool		btr_pcur_move_to_next_user_rec(btr_pcur_t* cursor, mtr_t* mtr);

void					btr_pcur_move_to_next_page(btr_pcur_t* cursor, mtr_t* mtr);

void					btr_pcur_move_backward_from_page(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE	btr_cur_t*	btr_pcur_get_btr_cur(btr_pcur_t* cursor);

UNIV_INLINE page_cur_t*	btr_pcur_get_page_cur(btr_pcur_t* cursor);

UNIV_INLINE page_t*		btr_pcur_get_page(btr_pcur_t* cursor);

UNIV_INLINE rec_t*		btr_pcur_get_rec(btr_pcur_t* cursor);

UNIV_INLINE ibool		btr_pcur_is_on_user_rec(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE ibool		btr_pcur_is_after_last_on_page(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE ibool		btr_pcur_is_before_first_on_page(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE ibool		btr_pcur_is_before_first_in_tree(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE ibool		btr_pcur_is_after_last_in_tree(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE void		btr_pcur_move_to_next_on_page(btr_pcur_t* cursor, mtr_t* mtr);

UNIV_INLINE void		btr_pcur_move_to_prev_on_page(btr_pcur_t* cursor, mtr_t* mtr);

#include "btr0pcur.inl"

#endif





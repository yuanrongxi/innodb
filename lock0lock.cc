#include "lock0lock.h"
#include "usr0sess.h"
#include "trx0purge.h"

#define LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK	1000000

#define LOCK_RELEASE_KERNEL_INTERVAL		1000

#define LOCK_PAGE_BITMAP_MARGIN				64


ibool lock_print_waits = FALSE;
/*事务锁系统句柄*/
lock_sys_t* lock_sys = NULL;

/*表事务锁对象定义*/
typedef struct lock_table_struct
{
	dict_table_t* table;			/*字典元数据中的表对象句柄*/
	UT_LIST_NODE_T(lock_t) locks;	/*在表上的事务锁列表*/
}lock_table_t;

typedef struct lock_rec_struct
{
	ulint		space;		/*记录所处的space的ID*/
	ulint		page_no;	/*记录所处的page页号*/
	ulint		n_bits;
}lock_rec_t;

struct lock_struct
{
	trx_t*			trx;
	ulint			type_mode;
	hash_node_t		hash;
	dict_index_t*	index;
	UT_LIST_NODE_T(lock_t) locks;
	union{
		lock_table_t	tab_lock;/*表锁*/
		lock_rec_t		rec_lock;/*行锁*/
	}un_member;
};

/*死锁标识*/
ibool lock_deadlock_found = FALSE;
/*错误信息缓冲区，5000字节*/
char* lock_latest_err_buf;


static ibool		lock_deadlock_occurs(lock_t* lock, trx_t* trx);
static ibool		lock_deadlock_recursive(trx_t* start, trx_t* trx, lock_t* wait_lock, ulint cost);

/************************************************************************/
/*kernel_mutex是在srv0srv.h定义的全局内核锁*/
UNIV_INLINE void lock_mutex_enter_kernel()
{
	mutex_enter(&kernel_mutex);
}

UNIV_INLINE void lock_mutex_exit_kernel()
{
	mutex_exit(&kernel_mutex);
}

/*检查记录是否可以进行一致性读*/
ibool lock_clust_rec_cons_read_sees(rec_t* rec, dict_index_t* index, read_view_t* view)
{
	dulint	trx_id;

	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(page_rec_is_user_rec(rec));

	trx_id = row_get_rec_trx_id(rec, index);
	if(read_view_sees_trx_id(view, trx_id))
		return TRUE;

	return FALSE;
}

/*检查非聚合索引记录是否可以进行一致性读*/
ulint  lock_sec_rec_cons_read_sees(rec_t* rec, dict_index_t* index, read_view_t* view)
{
	dulint	max_trx_id;

	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(page_rec_is_user_rec(rec));

	if(recv_recovery_is_on()) /*检查redo log是否在进行日志恢复*/
		return FALSE;

	/*获得对应记录的max trx id*/
	max_trx_id = page_get_max_trx_id(buf_frame_align(rec));
	if(ut_dulint_cmp(max_trx_id, view->up_limit_id) >= 0)
		return FALSE;

	return TRUE;
}

/*建立一个系统锁对象*/
void lock_sys_create(ulint n_cells)
{
	/*创建lock sys对象*/
	lock_sys = mem_alloc(sizeof(lock_sys_t));
	/*创建一个lock_sys中的哈希表*/
	lock_sys->rec_hash = hash_create(n_cells);
	/*分配锁错误信息的缓冲区*/
	lock_latest_err_buf = mem_alloc(5000);
}

ulint lock_get_size()
{
	return (ulint)(sizeof(lock_t));
}

/*获得事务锁的模式(IS, IX, S, X, NONE)*/
UNIV_INLINE ulint lock_get_mode(lock_t* lock)
{
	ut_ad(lock);
	return lock->type_mode & LOCK_MODE_MASK;
}

/*获得锁的类型(table lock, rec lock)*/
UNIV_INLINE ulint lock_get_type(lock_t* lock)
{
	ut_ad(lock);
	return lock->type_mode & LOCK_TYPE_MASK;
}

/*获得锁是否在等待状态*/
UNIV_INLINE ibool lock_get_wait(lock_t* lock)
{
	ut_ad(lock);
	if(lock->type_mode & LOCK_WAIT) /*锁处于等待状态*/
		return TRUE;

	return FALSE;
}

/*设置事务锁的等待*/
UNIV_INLINE void lock_set_lock_and_trx_wait(lock_t* lock, trx_t* trx)
{
	ut_ad(lock);
	ut_ad(trx->wait_lock == NULL);

	trx->wait_lock = lock;
	lock->type_mode = lock->type_mode | LOCK_WAIT;
}

/*复位事务锁的等待状态*/
UNIV_INLINE void lock_reset_lock_and_trx_wait(lock_t* lock)
{
	ut_ad((lock->trx)->wait_lock == lock);
	ut_ad(lock_get_wait(lock));

	(lock->trx)->wait_lock = NULL;
	lock->type_mode = lock->type_mode & ~LOCK_WAIT;
}

/*获得事务锁的记录范围锁定状态*/
UNIV_INLINE ibool lock_rec_get_gap(lock_t* lock)
{
	ut_ad(lock);
	ut_ad(lock_get_type(lock) == LOCK_REC);

	if(lock->type_mode & LOCK_GAP)
		return TRUE;

	return FALSE;
}

/*设置事务锁的记录范围锁定状态*/
UNIV_INLINE void lock_rec_set_gap(lock_t* lock, ibool val)
{
	ut_ad(lock);
	ut_ad((val == TRUE) || (val == FALSE));
	ut_ad(lock_get_type(lock) == LOCK_REC);

	if(val)
		lock->type_mode = lock->type_mode | LOCK_GAP;
	else
		lock->type_mode = lock->type_mode & ~LOCK_GAP;
}

/*判断事务锁的mode1是否比mode2更控制的严，一般LOCK_X > LOCK_S > LOCK_IX > LOCK_IS*/
UNIV_INLINE ibool lock_mode_stronger_or_eq(ulint mode1, ulint mode2)
{
	ut_ad(mode1 == LOCK_X || mode1 == LOCK_S || mode1 == LOCK_IX
		|| mode1 == LOCK_IS || mode1 == LOCK_AUTO_INC);

	ut_ad(mode2 == LOCK_X || mode2 == LOCK_S || mode2 == LOCK_IX
		|| mode2 == LOCK_IS || mode2 == LOCK_AUTO_INC);
	
	if(mode1 == LOCK_X)
		return TRUE;
	else if(mode1 == LOCK_AUTO_INC && mode2 == LOCK_AUTO_INC)
		return TRUE;
	else if(mode1 == LOCK_S && (mode2 == LOCK_S || mode2 == LOCK_IS))
		return TRUE;
	else if(mode1 == LOCK_IX && (mode2 == LOCK_IX || mode2 == LOCK_IS))
		return TRUE;

	return FALSE;
}

/*判断事务锁的mode1是否兼容mode2模式
		AINC	IS		IX		S		X
AINC	n		y		y		n		n
IS		y		y		y		y		n
IX		y		y		y		n		n
S		n		y		n		y		n
X		n		n		n		n		n
*****************************************/
UNIV_INLINE ibool lock_mode_compatible(ulint mode1, ulint mode2)
{
	ut_ad(mode1 == LOCK_X || mode1 == LOCK_S || mode1 == LOCK_IX
		|| mode1 == LOCK_IS || mode1 == LOCK_AUTO_INC);
	ut_ad(mode2 == LOCK_X || mode2 == LOCK_S || mode2 == LOCK_IX
		|| mode2 == LOCK_IS || mode2 == LOCK_AUTO_INC);

	/*共享锁是兼容共享锁和意向共享锁的*/
	if(mode1 == LOCK_S && (mode2 == LOCK_IS || mode2 == LOCK_S))
		return TRUE;
	/*独享锁是不兼容任何其他形式的锁*/
	else if(mode1 == LOCK_X) 
		return FALSE;
	/*自增长锁是兼容意向锁的*/
	else if(mode1 == LOCK_AUTO_INC && (mode2 == LOCK_IS || mode2 == LOCK_IX))
		return TRUE;
	/*意向共享锁模式兼容共享锁、意向独享锁、自增长锁和共享意向锁*/
	else if(mode1 == LOCK_IS && (mode2 == LOCK_IS || mode2 == LOCK_IX
								 || mode2 == LOCK_AUTO_INC || mode2 == LOCK_S))
		return TRUE;
	/*意向独占锁模式兼容意向共享锁、自增长锁和意向独占锁*/
	else if(mode1 == LOCK_IX &&(mode2 == LOCK_IS || mode2 == LOCK_AUTO_INC || mode2 == LOCK_IX))
		return TRUE;

	return FALSE;							 
}

/*加入mode == LOCK_S,返回LOCK_X*/
UNIV_INLINE ulint lock_get_confl_mode(ulint mode)
{
	ut_ad(mode == LOCK_X || mode == LOCK_S);
	if(mode == LOCK_S)
		return LOCK_X;
	
	return LOCK_S;
}

/*判断lock1是否和lock2兼容*/
UNIV_INLINE ibool lock_has_to_wait(lock_t* lock1, lock_t* lock2)
{
	if(lock1->trx != lock2->trx && !lock_mode_compatible(lock_get_mode(lock1), lock_get_mode(lock2)))
		return TRUE;
	return FALSE;
}

/*获得记录行锁的bits*/
UNIV_INLINE ulint lock_rec_get_n_bits(lock_t* lock)
{
	return lock->un_member.rec_lock.n_bits;
}

UNIV_INLINE ibool lock_rec_get_nth_bit(lock_t* lock, ulint i)
{
	ulint	byte_index;
	ulint	bit_index;
	ulint	b;

	ut_ad(lock);
	ut_ad(lock_get_type(lock) == LOCK_REC);

	if(i >= lock->un_member.rec_lock.n_bits)
		return FALSE;

	byte_index = i / 8;
	bit_index = i % 8;

	/*进行bitmap定位*/
	b = (ulint)*((byte*)lock + sizeof(lock_t) + byte_index);
	return ut_bit_get_nth(b, bit_index);
}

UNIV_INLINE void lock_rec_set_nth_bit(lock_t* lock, ulint i)
{
	ulint	byte_index;
	ulint	bit_index;
	byte*	ptr;
	ulint	b;

	ut_ad(lock);
	ut_ad(lock_get_type(lock) == LOCK_REC);
	ut_ad(i < lock->un_member.rec_lock.n_bits);

	byte_index = i / 8;
	bit_index = i % 8;

	ptr = (byte*)lock + sizeof(lock_t) + byte_index;
	b = (ulint)*ptr;
	b = ut_bit_set_nth(b, bit_index, TRUE);
	*ptr = (byte)b;
}

/*获得rec lock bitmap第一个有效位的序号*/
static ulint lock_rec_find_set_bit(lock_t* lock)
{
	ulint i;
	for(i = 0; i < lock_rec_get_n_bits(lock); i ++){
		if(lock_rec_get_nth_bit(lock, i))
			return i;
	}

	return ULINT_UNDEFINED;
}

UNIV_INLINE void lock_rec_reset_nth_bit(lock_t* lock, ulint i)
{
	ulint	byte_index;
	ulint	bit_index;
	byte*	ptr;
	ulint	b;

	ut_ad(lock);
	ut_ad(lock_get_type(lock) == LOCK_REC);
	ut_ad(i < lock->un_member.rec_lock.n_bits);

	byte_index = i / 8;
	bit_index = i % 8;

	ptr = (byte*)lock + sizeof(lock_t) + byte_index;
	b = (ulint)*ptr;
	b = ut_bit_set_nth(b, bit_index, FALSE);
	*ptr = (byte)b;
}

/*获取也得第行记录的下一个锁*/
UNIV_INLINE lock_t* lock_rec_get_next_on_page(lock_t* lock)
{
	ulint	space;
	ulint	page_no;

	ut_ad(mutex_own(&kernel_mutex));

	space = lock->un_member.rec_lock.space;
	page_no = lock->un_member.rec_lock.page_no;

	for(;;){
		lock = HASH_GET_NEXT(hash, lock);
		if(lock == NULL)
			break;

		/*查找到了对应的*/
		if(lock->un_member.rec_lock.space == space && lock->un_member.rec_lock.page_no = page_no)
			break;
	}

	return lock;
}

/*获得page的第一个锁*/
UNIV_INLINE lock_t* lock_rec_get_first_on_page_addr(ulint space, ulint page_no)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));

	lock = HASH_GET_FIRST(lock_sys->rec_hash, lock_rec_hash(space, page_no));
	while(lock){
		if ((lock->un_member.rec_lock.space == space) 
			&& (lock->un_member.rec_lock.page_no == page_no))
			break;

		lock = HASH_GET_NEXT(hash, lock);
	}

	return lock;
}

/*判断space page_no对应的页是否有锁*/
ibool lock_rec_expl_exist_on_page(ulint space, ulint page_no)
{
	ibool ret;

	mutex_enter(&kernel_mutex);
	if(lock_rec_get_first_on_page_addr(space, page_no))
		ret = TRUE;
	else
		ret = FALSE;

	mutex_exit(&kernel_mutex);

	return ret;
}

/*获得ptr所在页的第一个锁*/
UNIV_INLINE lock_t* lock_rec_get_first_on_page(byte* ptr)
{
	ulint	hash;
	lock_t*	lock;
	ulint	space;
	ulint	page_no;

	ut_ad(mutex_own(&kernel_mutex));

	hash = buf_frame_get_lock_hash_val(ptr);
	lock = HASH_GET_FIRST(lock_sys->rec_hash, hash);
	while(lock){
		space = buf_frame_get_space_id(ptr);
		page_no = buf_frame_get_page_no(ptr);

		if(space == lock->un_member.rec_lock.space && page_no == lock->un_member.rec_lock.page_no)
			break;

		lock = HASH_GET_NEXT(hash, lock);
	}

	return lock;
}

/*获得行记录的下一个显式锁*/
UNIV_INLINE lock_t* lock_rec_get_next(rec_t* rec, lock_t* lock)
{
	ut_ad(mutex_own(&kernel_mutex));

	for(;;){
		lock = lock_rec_get_next_on_page(lock);
		if(lock == NULL)
			return NULL;

		if(lock_rec_get_nth_bit(lock, rec_get_heap_no(rec)))
			return lock;
	}
}

UNIV_INLINE lock_t* lock_rec_get_first(rec_t* rec)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));
	lock = lock_rec_get_first_on_page(rec);
	while(lock){
		if(lock_rec_get_nth_bit(lock, rec_get_heap_no(rec))) /*判断lock的bitmap是否有对应的位状态*/
			break;

		lock = lock_rec_get_next_on_page(lock);
	}

	return lock;
}

static void lock_rec_bitmap_reset(lock_t* lock)
{
	byte*	ptr;
	ulint	n_bytes;
	ulint	i;

	ptr = (byte*)lock + sizeof(lock_t);
	n_bytes = lock_rec_get_n_bits(lock) / 8;
	ut_ad(lock_rec_get_n_bits(lock) % 8 == 0);

	/*将lock的bitmap置为0*/
	for(i = 0; i < n_bytes; i ++){
		*ptr = 0;
		ptr ++;
	}
}

/*分配一个行记录锁，并将lock中的内容拷贝到新分配的行锁中*/
static lock_t* lock_rec_copy(lock_t* lock, mem_heap_t* heap)
{
	lock_t*	dupl_lock;
	ulint	size;

	/*获得lock对象占用的空间大小*/
	size = sizeof(lock_t) + lock_rec_get_n_bits(lock) / 8;
	dupl_lock = mem_heap_alloc(heap, size);

	ut_memcpy(dupl_lock, lock, size);

	return dupl_lock;
}

/*获得in_lock的前一个的行记录锁*/
static lock_t* lock_rec_get_prev(lock_t* in_lock, ulint heap_no)
{
	lock_t*	lock;
	ulint	space;
	ulint	page_no;
	lock_t*	found_lock 	= NULL;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(lock_get_type(in_lock) == LOCK_REC);

	space = in_lock->un_member.rec_lock.space;
	page_no = in_lock->un_member.rec_lock.page_no;

	lock = lock_rec_get_first_on_page_addr(space, page_no);
	for(;;){
		ut_ad(lock);

		if(lock == in_lock)
			return found_lock;

		if(lock_rec_get_nth_bit(lock, heap_no)) /*判断是否是本行记录的lock*/
			found_lock = lock;

		lock = lock_rec_get_next_on_page(lock);
	}
}


/************************************************************************/



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
	ulint		n_bits;		/*行锁的bitmap位数，lock_t结构后面会跟一个BUF，长度为n_bits / 8*/
}lock_rec_t;

/*锁对象*/
struct lock_struct
{
	trx_t*			trx;
	ulint			type_mode;
	hash_node_t		hash;
	dict_index_t*	index;
	UT_LIST_NODE_T(lock_t) trx_locks;
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

/*获取也得行记录的下一个锁,在同一个page中*/
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

/*获得行记录的下一个锁*/
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

/*判断事务trx是否持有table的锁比mode更严格*/
UNIV_INLINE lock_t* lock_table_has(trx_t* trx, dict_table_t* table, ulint mode)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));

	/*从后面扫描到前面, 有可能事务已经持有了锁*/
	lock = UT_LIST_GET_LAST(table->locks);
	while(lock != NULL){
		if(lock->trx == trx && lock_mode_stronger_or_eq(lock_get_mode(lock), mode)){
			ut_ad(!lock_get_wait(lock));
			return lock;
		}

		lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, lock);
	}
}

/*获得一个比mode更严格的rec行记录锁，这个锁必须是trx发起的，并且处于non_gap状态*/
UNIV_INLINE lock_t* lock_rec_has_expl(ulint mode, rec_t* rec, trx_t* trx)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad((mode == LOCK_X) || (mode == LOCK_S));

	lock = lock_rec_get_first(rec);
	while(lock != NULL){
		if(lock->trx == trx && lock_mode_stronger_or_eq(lock_get_mode(lock), mode)
			&& !lock_get_wait(lock) && !lock_rec_get_gap(lock) || page_rec_is_supremum(rec))
			return lock;

		lock = lock_rec_get_next(rec, lock);
	}
}

/*检查是否除trx以外的事务有持有比mode更严格的锁，*/
UNIV_INLINE lock_t* lock_rec_other_has_expl_req(ulint mode, ulint gap, ulint wait, rec_t* rec, trx_t* trx)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad((mode == LOCK_X) || (mode == LOCK_S));

	lock = lock_rec_get_first(rec);
	while(lock != NULL){
		if(lock->trx != trx && (gap || !(lock_rec_get_gap(lock) || page_rec_is_supremum(rec))
			&& (wait || !lock_get_wait(lock)) && lock_mode_stronger_or_eq(lock_get_mode(lock), mode)))
			return lock;

		lock = lock_rec_get_next(rec, lock);
	}

	return NULL;
}

/*在记录所在的page中，查找trx事务发起的type_mode模式的的锁*/
UNIV_INLINE lock_t* lock_rec_find_similar_on_page(ulint type_mode, rec_t* rec, trx_t* trx)
{
	lock_t*	lock;
	ulint	heap_no;

	ut_ad(mutex_own(&kernel_mutex));

	heap_no = rec_get_heap_no(rec);
	lock = lock_rec_get_first_on_page(rec);
	while(lock != NULL){
		if(lock->trx == trx && lock->type_mode == type_mode
			&& lock_rec_get_n_bits(lock) > heap_no)
			return lock;

		lock = lock_rec_get_next_on_page(lock);
	}

	return NULL;
}

/*返回在记录rec的二级索引上存储LOCK_IX,返回发起这个锁的事务trx_t*/
trx_t* lock_sec_rec_some_has_impl_off_kernel(rec_t* rec, dict_index_t* index)
{
	page_t*	page;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(!(index->type & DICT_CLUSTERED));
	ut_ad(page_rec_is_user_rec(rec));

	/*获得rec对应的page*/
	page = buf_frame_align(rec);

	if(!(ut_dulint_cmp(page_get_max_trx_id(page), trx_list_get_min_trx_id()) >= 0) 
		&& !recv_recovery_is_on())
		return NULL;

	return row_vers_impl_x_locked_off_kernel(rec, index);
}

/*建立一个记录行锁*/
static lock_t* lock_rec_create(ulint type_mode, rec_t* rec, dict_index* index, trx_t* trx)
{
	page_t*	page;
	lock_t*	lock;
	ulint	page_no;
	ulint	heap_no;
	ulint	space;
	ulint	n_bits;
	ulint	n_bytes;

	ut_ad(mutex_own(&kernel_mutex));

	page = buf_frame_align(rec);
	space = buf_frame_get_space_id(page);
	page_no	= buf_frame_get_page_no(page);
	heap_no = rec_get_heap_no(rec);

	/*supremum记录是不能使用LOCK_GAP范围锁的*/
	if(rec == page_get_supremum_rec(page))
		type_mode = type_mode & ~LOCK_GAP;

	/*计算rec_lock bitmap,是page已经分配出去的记录数 + 阈值修正值64（为了缓冲区安全）*/
	n_bits = page_header_get_field(page, PAGE_N_HEAP) + LOCK_PAGE_BITMAP_MARGIN;
	n_bytes = n_bits / 8 + 1;

	/*分配lock_t*/
	lock = mem_heap_alloc(trx->lock_heap, sizeof(lock_t) + n_bytes);
	if(lock == NULL)
		return NULL;

	/*将lock加入到trx事务对象中*/
	UT_LIST_ADD_LAST(trx_locks, trx->trx_locks, lock);
	lock->trx = trx;

	lock->type_mode = (type_mode & ~LOCK_TYPE_MASK) | LOCK_REC;
	lock->index = index;

	lock->un_member.rec_lock.space = space;
	lock->un_member.rec_lock.page_no = page_no;
	lock->un_member.rec_lock.n_bits = n_bytes * 8;

	/*初始化lock bitmap*/
	lock_rec_bitmap_reset(lock);
	lock_rec_set_nth_bit(lock, heap_no);

	HASH_INSERT(lock_t, hash, lock_sys->rec_hash, lock_rec_fold(space, page_no), lock);
	if(type_mode & LOCK_WAIT) /*设置wait flag*/
		lock_set_lock_and_trx_wait(lock, trx);

	return lock;
}

/*创建一个记录行锁，并进行lock wait状态排队，*/
static ulint lock_rec_enqueue_waiting(ulint type_mode, rec_t* rec, dict_index_t* index, que_thr_t* thr)
{
	lock_t* lock;
	trx_t* trx;

	ut_ad(mutex_own(&kernel_mutex));

	/*查询线程暂停*/
	if(que_thr_stop(thr)){
		ut_a(0);
		return DB_QUE_THR_SUSPENDED;
	}

	trx = thr_get_trx(thr);
	if(trx->dict_operation){
		ut_print_timestamp(stderr);
		fprintf(stderr,"  InnoDB: Error: a record lock wait happens in a dictionary operation!\n"
			"InnoDB: Table name %s. Send a bug report to mysql@lists.mysql.com\n", index->table_name);
	}

	/*创建一个行锁并进入lock wait状态*/
	lock = lock_rec_create(type_mode | LOCK_WAIT, rec, index, trx);
	/*检查死锁*/
	if(lock_deadlock_occurs(lock, trx)){
		/*进行lock wait复位*/
		lock_reset_lock_and_trx_wait(lock);
		lock_rec_reset_nth_bit(lock, rec_get_heap_no(rec));

		return DB_DEADLOCK;
	}

	trx->que_state = TRX_QUE_LOCK_WAIT;
	trx->wait_started = time(NULL);

	ut_a(que_thr_stop(thr));
	if(lock_print_waits)
		printf("Lock wait for trx %lu in index %s\n", ut_dulint_get_low(trx->id), index->name);

	return DB_LOCK_WAIT;
}

/*增加一个记录行锁，并将锁放入锁的队列中*/
static lock_t* lock_rec_add_to_queue(ulint type_mode, rec_t* rec, dict_index_t* index, trx_t* trx)
{
	lock_t*	lock;
	lock_t*	similar_lock	= NULL;
	ulint	heap_no;
	page_t*	page;
	ibool	somebody_waits	= FALSE;

	ut_ad(mutex_own(&kernel_mutex));

	/*对锁的严格度做检验*/
	ut_ad((type_mode & (LOCK_WAIT | LOCK_GAP))
		|| ((type_mode & LOCK_MODE_MASK) != LOCK_S)
		|| !lock_rec_other_has_expl_req(LOCK_X, 0, LOCK_WAIT, rec, trx));

	ut_ad((type_mode & (LOCK_WAIT | LOCK_GAP))
		|| ((type_mode & LOCK_MODE_MASK) != LOCK_X)
		|| !lock_rec_other_has_expl_req(LOCK_S, 0, LOCK_WAIT, rec, trx));

	type_mode = type_mode | LOCK_REC;
	page = buf_frame_align(rec);

	/*记录是supremum*/
	if(rec == page_get_supremum_rec(page)){
		type_mode = type_mode & ~LOCK_GAP;
	}

	heap_no = rec_get_heap_no(rec);
	lock = lock_rec_get_first_on_page(rec);

	/*查找行记录rec是否用处于lock wait状态的行锁*/
	while(lock != NULL){
		if(lock_get_wait(lock) && lock_rec_get_nth_bit(lock, heap_no))
			somebody_waits = TRUE;

		lock = lock_rec_get_next_on_page(lock);
	}

	/*查找trx事务发起模式为type_mode的记录行锁，必须是在rec所处的page中的行*/
	similar_lock = lock_rec_find_similar_on_page(type_mode, rec, trx);

	/*可以重用similar_lock*/
	if(similar_lock != NULL && !somebody_waits && !(type_mode & LOCK_WAIT)){
		lock_rec_set_nth_bit(similar_lock, heap_no);
		return similar_lock;
	}

	/*创建一个新的行锁，并进行排队*/
	return lock_rec_create(type_mode, rec, index, trx);
}

/*快速获得行锁的控制权，大部分流程都是这样的*/
UNIV_INLINE ibool lock_rec_lock_fast(ibool impl, ulint mode, rec_t* rec, dict_index_t* index, que_thr_t* thr)
{
	lock_t*	lock;
	ulint	heap_no;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(mode == LOCK_X || mode == LOCK_S);

	heap_no = rec_get_heap_no(rec);
	lock = lock_rec_get_first_on_page(rec);
	if(lock == NULL){ /*page中没有其他的锁, 创建一个mode类型的锁*/
		if(!impl)
			lock_rec_create(mode, rec, index, thr_get_trx(thr));
		return TRUE;
	}

	/*page的有多个LOCK*/
	if(lock_rec_get_next_on_page(lock))
		return FALSE;

	/*lock的事务与thr中的trx不相同、或者不是行锁、或者lock的记录与rec不相同，直接返回*/
	if(lock->trx != thr_get_trx(thr) || lock->type_mode != (mode | LOCK_REC) 
		|| lock_rec_get_n_bits(lock) <= heap_no)
			return FALSE;

	/*有且只有个行锁，并且这个行锁指向的记录rec,直接认为可以获得锁权*/
	if(!impl)
		lock_rec_set_nth_bit(lock, heap_no);

	return TRUE;
}

/*从排队队列中选择lock,并获得锁权,会检查锁的兼容性*/
static ulint lock_rec_lock_slow(ibool impl, ulint mode, rec_t* rec, dict_index_t* index, que_thr_t* thr)
{
	ulint	confl_mode;
	trx_t*	trx;
	ulint	err;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad((mode == LOCK_X) || (mode == LOCK_S));

	trx = thr_get_trx(thr);
	confl_mode = lock_get_confl_mode(mode);

	ut_ad((mode != LOCK_S) || lock_table_has(trx, index->table, LOCK_IS));
	ut_ad((mode != LOCK_X) || lock_table_has(trx, index->table, LOCK_IX));

	/*trx有比mode更加严格的锁模式存在rec行锁*/
	if(lock_rec_has_expl(mode, rec, trx))
		err = DB_SUCCESS;
	else if(lock_rec_other_has_expl_req(confl_mode, 0, LOCK_WAIT, rec, trx)) /*其他事务有更严格的锁在rec行上*/
		err = lock_rec_enqueue_waiting(mode, rec, index, thr); /*创建一个新锁并进行等待*/
	else{
		if(!impl) /*增加一个行锁，并加入到锁队列中*/
			lock_rec_add_to_queue(LOCK_REC | mode, rec, index, trx);
		err = DB_SUCCESS;
	}

	return err;
}

ulint lock_rec_lock(ibool impl, ulint mode, rec_t* rec, dict_index_t* index, que_thr_t* thr)
{
	ulint	err;

	ut_ad(mutex_own(&kernel_mutex));

	ut_ad((mode != LOCK_S) || lock_table_has(thr_get_trx(thr), index->table, LOCK_IS));
	ut_ad((mode != LOCK_X) || lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));

	/*先尝试快速加锁*/
	if(lock_rec_lock_fast(impl, mode, rec, index, thr))
		err = DB_SUCCESS;
	else
		err = lock_rec_lock_slow(impl, mode, rec, index, thr);

	return err;
}

/*检查wait_lock是否还有指向同一行记录并且不兼容的锁，也就是判断wait lock是否要进行等待*/
static ibool lock_rec_has_to_wait_in_queue(lock_t* wait_lock)
{
	lock_t*	lock;
	ulint	space;
	ulint	page_no;
	ulint	heap_no;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(lock_get_wait(wait_lock));

	/*获得的定位信息space page rec*/
	space = wait_lock->un_member.rec_lock.space;
	page_no = wait_lock->un_member.rec_lock.page_no;
	heap_no = lock_rec_find_set_bit(wait_lock);

	lock = lock_rec_get_first_on_page_addr(space, page_no);
	while(lock != wait_lock){
		/*wait_lock和lock不兼容，并且处于同一rec记录行*/
		if (lock_has_to_wait(wait_lock, lock) && lock_rec_get_nth_bit(lock, heap_no))
			return TRUE;

		lock = lock_rec_get_next_on_page(lock);
	}

	return FALSE;
}

/*授予锁权*/
void lock_grant(lock_t* lock)
{
	ut_ad(mutex_own(&kernel_mutex));

	/*锁的lock wait标识复位*/
	lock_reset_lock_and_trx_wait(lock);

	/*主键自增长锁模式*/
	if(lock_get_mode(lock) == LOCK_AUTO_INC){
		if(lock->trx->auto_inc_lock != NULL)
			fprintf(stderr, "InnoDB: Error: trx already had an AUTO-INC lock!\n");
		
		lock->trx->auto_inc_lock = lock;
	}

	if(lock_print_waits)
		printf("Lock wait for trx %lu ends\n", ut_dulint_get_low(lock->trx->id));

	/*结束事务等待*/
	trx_end_lock_wait(lock->trx);
}

/*取消正在等待的锁*/
static void lock_rec_cancel(lock_t* lock)
{
	ut_ad(mutex_own(&kernel_mutex));

	/*复位锁的bitmap位*/
	lock_rec_reset_nth_bit(lock, lock_rec_find_set_bit(lock));
	/*复位锁的lock wait状态*/
	lock_reset_lock_and_trx_wait(lock);

	/*结束对应事物的等待*/
	trx_end_lock_wait(lock->trx);
}

/*将in_lock从lock_sys中删除， 并激活其对应页的一个等待行锁, in_lock可能是一个waiting或者granted状态的锁*/
void lock_rec_dequeue_from_page(lock_t* in_lock)
{
	ulint	space;
	ulint	page_no;
	lock_t*	lock;
	trx_t*	trx;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(lock_get_type(in_lock) == LOCK_REC);

	trx = in_lock->trx;

	space = in_lock->un_member.rec_lock.space;
	page_no = in_lock->un_member.rec_lock.page_no;

	/*将in_lock从lock_sys和trx中删除*/
	HASH_DELETE(lock_t, hash, lock_sys->rec_hash, lock_rec_fold(space, page_no), in_lock);
	UT_LIST_REMOVE(trx_locks, trx->trx_locks, in_lock);

	/*激活同一个页中可以激活(grant)的行锁*/
	lock = lock_rec_get_first_on_page_addr(space, page_no);
	while(lock != NULL){
		if(lock_get_wait(lock) && !lock_rec_has_to_wait_in_queue(lock)) /*lock处于等待状态并且其指向的行记录没有其他的排斥锁*/
			lock_grant(lock);

		lock = lock_rec_get_next_on_page(lock);
	}
}
/*将in_lock从lock_sys中删除, in_lock可能是一个waiting或者granted状态的锁*/
static void lock_rec_discard(lock_t* in_lock)
{
	ulint	space;
	ulint	page_no;
	trx_t*	trx;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(lock_get_type(in_lock) == LOCK_REC);

	trx = in_lock->trx;

	space = in_lock->un_member.rec_lock.space;
	page_no = in_lock->un_member.rec_lock.page_no;

	HASH_DELETE(lock_t, hash, lock_sys->rec_hash, lock_rec_fold(space, page_no), in_lock);
	UT_LIST_REMOVE(trx_locks, trx->trx_locks, in_lock);
}

/*遗弃page中所有行记录锁*/
static void lock_rec_free_all_from_discard_page(page_t* page)
{
	ulint	space;
	ulint	page_no;
	lock_t*	lock;
	lock_t*	next_lock;

	ut_ad(mutex_own(&kernel_mutex));

	space = buf_frame_get_space_id(page);
	page_no = buf_frame_get_page_no(page);

	lock = lock_rec_get_first_on_page_addr(space, page_no);
	while(lock != NULL){
		ut_ad(lock_rec_find_set_bit(lock) == ULINT_UNDEFINED);
		ut_ad(!lock_get_wait(lock));

		next_lock = lock_rec_get_next_on_page(lock);

		lock_rec_discard(lock);
		lock = next_lock;
	}
}

/*复位rec的锁bitmap,并取消对应锁事务的等待*/
void lock_rec_reset_and_release_wait(rec_t* rec)
{
	lock_t* lock;
	ulint heap_no;

	ut_ad(mutex_own(&kernel_mutex));
	
	/*获得记录序号*/
	heap_no = rec_get_heap_no(rec);
	lock = lock_rec_get_first(rec);
	while(lock != NULL){
		if(lock_get_wait(lock))
			lock_rec_cancel(lock);
		else
			lock_rec_reset_nth_bit(lock, heap_no);

		lock = lock_rec_get_next(rec, lock);
	}
}

/*heir记录行锁继承rec所有的记录行锁，将lock wait锁继承为GAP范围锁*/
void lock_rec_inherit_to_gap(rec_t* heir, rec_t* rec)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));

	lock = lock_rec_get_first(rec);
	while(lock != NULL){
		lock_rec_add_to_queue((lock->type_mode | LOCK_GAP) & ~LOCK_WAIT, heir, lock->index, lock->trx);
		lock = lock_rec_get_next(rec, lock);
	}
}

/*将donator记录的行锁转移到receiver记录上，并且复位donator的锁bitmap*/
static void lock_rec_move(rec_t* receiver, rec_t* donator)
{
	lock_t*	lock;
	ulint	heap_no;
	ulint	type_mode;

	ut_ad(mutex_own(&kernel_mutex));

	heap_no = rec_get_heap_no(donator);
	lock = lock_rec_get_first(donator);
	/*reciver必须是没有任何行锁的*/
	ut_ad(lock_rec_get_first(receiver) == NULL);

	while(lock != NULL){
		type_mode = lock->type_mode;
		lock_rec_reset_nth_bit(lock, heap_no);

		if(lock_get_wait(lock))
			lock_reset_lock_and_trx_wait(lock);

		/*在receiver记录上增加对应模式的行锁*/
		lock_rec_add_to_queue(type_mode, receiver, lock->index, lock->trx);
		lock = lock_rec_get_next(donator, lock);
	}

	/*donator的行锁应该被完全清除*/
	ut_ad(lock_rec_get_first(donator) == NULL);
}

/*保留同时存在old page和page中的行锁，并重组这些行锁到page对应的行记录上,复杂度O(m * n)*/
void lock_move_reorganize_page(page_t* page, page_t* old_page)
{
	lock_t*		lock;
	lock_t*		old_lock;
	page_cur_t	cur1;
	page_cur_t	cur2;
	ulint		old_heap_no;
	
	mem_heap_t*	heap = NULL;
	rec_t*		sup;

	UT_LIST_BASE_NODE_T(lock_t)	old_locks;

	lock_mutex_enter_kernel();

	lock = lock_rec_get_first_on_page(page);
	if(lock == NULL){
		lock_mutex_enter_kernel();
		return;
	}

	heap = mem_heap_create(256);

	UT_LIST_INIT(old_locks);

	/*将page所有的行锁复制到old_locks中，并且清空*/
	while(lock != NULL){
		old_lock = lock_rec_copy(lock, heap);
		UT_LIST_ADD_LAST(trx_locks, old_locks, old_lock);

		lock_rec_bitmap_reset(lock);
		if(lock_get_wait(lock))
			lock_reset_lock_and_trx_wait(lock);

		lock = lock_rec_get_next_on_page(lock);
	}

	sup = page_get_supremum_rec(page);
	
	lock = UT_LIST_GET_FIRST(old_locks);
	while(lock){
		/*将页游标定位到页的起始记录*/
		page_cur_set_before_first(page, &cur1);
		page_cur_set_before_first(old_page, &cur2);

		/*全部扫描old page中的所有记录，并将对应的所转移并重组到page的记录中,行记录锁必须等待锁定old page中的记录*/
		for(;;){
			ut_ad(0 == ut_memcmp(page_cur_get_rec(&cur1), page_cur_get_rec(&cur2), rec_get_data_size(page_cur_get_rec(&cur2))));

			old_heap_no = rec_get_heap_no(page_cur_get_rec(&cur2));
			if(lock_rec_get_nth_bit(lock, old_heap_no))
				lock_rec_add_to_queue(lock->type_mode, page_cur_get_rec(&cur1), lock->index, lock->trx);

			if(page_cur_get_rec(&cur1) == sup)
				break;

			page_cur_move_to_next(&cur1);
			page_cur_move_to_next(&cur2);
		}

		lock = UT_LIST_GET_NEXT(trx_locks, lock);
	}

	lock_mutex_exit_kernel();

	mem_heap_free(heap);
}

/*将所有page中rec之后(包括rec)的行锁转移到new page上，并取消在page上对应的行锁,new page从起始行开始,复杂度O(m * n)*/
void lock_move_rec_list_end(page_t* new_page, page_t* page, rec_t* rec)
{
	lock_t*		lock;
	page_cur_t	cur1;
	page_cur_t	cur2;
	ulint		heap_no;
	rec_t*		sup;
	ulint		type_mode;

	lock_mutex_enter_kernel();

	sup = page_get_supremum_rec(page);

	lock = lock_rec_get_first_on_page(page);
	while(lock != NULL){
		/*定位rec所在page的游标起始位置*/
		page_cur_position(rec, &cur1);
		if(page_cur_is_before_first(&cur1))
			page_cur_move_to_next(&cur1);

		page_cur_set_before_first(new_page, &cur2);
		page_cur_move_to_next(&cur2);

		while(page_cur_get_rec(&cur1) != sup){
			ut_ad(0 == ut_memcmp(page_cur_get_rec(&cur1), page_cur_get_rec(&cur2), rec_get_data_size(page_cur_get_rec(&cur2))));

			heap_no = rec_get_heap_no(page_cur_get_rec(&cur1));
			if(lock_rec_get_nth_bit(lock, heap_no)){
				type_mode = lock->type_mode;

				/*清除在lock的状态*/
				lock_rec_reset_nth_bit(lock, heap_no);
				if(lock_get_wait(lock))
					lock_reset_lock_and_trx_wait(lock);

				/*在cur2对应的行记录上加上一个对应的锁*/
				lock_rec_add_to_queue(type_mode, page_cur_get_rec(&cur2), lock->index, lock->trx);
			}

			page_cur_move_to_next(&cur1);
			page_cur_move_to_next(&cur2);
		}

		lock = lock_rec_get_next_on_page(lock);
	}

	lock_mutex_exit_kernel();
}
/*将所有page中rec之前(不包括rec)的行锁转移到new page上，并取消在page上对应的行锁, 新记录行从old_end开始,复杂度O(m * n)*/
void lock_move_rec_list_start(page_t* new_page, page_t* page, rec_t* rec, rec_t* old_end)
{
	lock_t*		lock;
	page_cur_t	cur1;
	page_cur_t	cur2;
	ulint		heap_no;
	ulint		type_mode;

	ut_ad(new_page);

	lock_mutex_enter_kernel();

	lock = lock_rec_get_first_on_page(page);
	while(lock != NULL){
		page_cur_set_before_first(page, &cur1);
		page_cur_move_to_next(&cur1);

		page_cur_position(old_end, &cur2);
		page_cur_move_to_next(&cur2);

		while(page_cur_get_rec(&cur1) != rec){
			ut_ad(0 == ut_memcmp(page_cur_get_rec(&cur1), page_cur_get_rec(&cur2), rec_get_data_size(page_cur_get_rec(&cur2))));

			heap_no = rec_get_heap_no(page_cur_get_rec(&cur1));
			if(lock_rec_get_nth_bit(lock, heap_no)){
				type_mode = lock->type_mode;

				lock_rec_reset_nth_bit(lock, heap_no);
				if(lock_get_wait(lock))
					lock_reset_lock_and_trx_wait(lock);

				lock_rec_add_to_queue(type_mode, page_cur_get_rec(&cur2), lock->index, lock->trx);
			}

			page_cur_move_to_next(&cur1);
			page_cur_move_to_next(&cur2);
		}

		lock = lock_rec_get_next_on_page(lock);
	}

	lock_mutex_exit_kernel();
}

/*page进行了右边分裂,对其记录行锁做调整*/
void lock_update_split_right(page_t* right_page, page_t* left_page)
{
	lock_mutex_enter_kernel();
	/*将left page的supremum的行锁转移到right page上*/
	lock_rec_move(page_get_supremum_rec(right_page), page_get_supremum_rec(left_page));
	/*left page的supremum继承right page的第一条记录行锁*/
	lock_rec_inherit_to_gap(page_get_supremum_rec(left_page), page_rec_get_next(page_get_infimum_rec(right_page)));

	lock_mutex_exit_kernel();
}

void lock_update_merge_right(rec_t* orig_succ, page_t* left_page)
{
	lock_mutex_enter_kernel();

	/*将left page所有的行锁作转化成一个gap 范围锁，并通过right page的第一条记录上做继承，
	这样就可以释放所有之前left page上的行锁，相当于锁升级*/
	lock_rec_inherit_to_gap(orig_succ, page_get_supremum_rec(left_page));

	/*因为是对left page中的锁升级，所以直接可以释放掉原来的行锁*/
	lock_rec_reset_and_release_wait(page_get_supremum_rec(left_page));
	lock_rec_free_all_from_discard_page(left_page);

	lock_mutex_exit_kernel();
}

/*锁升级到root page上*/
void lock_update_root_raise(page_t* new_page, page_t* root)
{
	lock_mutex_enter_kernel();

	lock_rec_move(page_get_supremum_rec(new_page), page_get_supremum_rec(root));

	lock_mutex_exit_kernel();
}

void lock_update_copy_and_discard(page_t* new_page, page_t* page)
{
	lock_mutex_enter_kernel();

	/*将page的锁全部扩大到new_page的supremum上，相当于GAP范围升级*/
	lock_rec_move(page_get_supremum_rec(new_page), page_get_supremum_rec(page));
	lock_rec_free_all_from_discard_page(page);

	lock_mutex_exit_kernel();
}

void lock_update_split_left(page_t* right_page, rec_t* left_page)
{
	lock_mutex_enter_kernel();

	/*将left page的supremum继承right page的第一行记录所有锁*/
	lock_rec_inherit_to_gap(page_get_supremum_rec(left_page), page_rec_get_next(page_get_infimum_rec(right_page)));

	lock_mutex_exit_kernel();
}

void lock_update_merge_left(page_t* left_page, rec_t* orig_pred, page_t* right_page)
{
	lock_mutex_enter_kernel();

	if(page_rec_get_next(orig_pred) != page_get_supremum_rec(left_page)){
		/*将orig_pred继承left page的supremum的行锁，orig_pred是suprmum的前一条记录*/
		lock_rec_inherit_to_gap(page_rec_get_next(orig_pred), page_get_supremum_rec(left_page));
		/*释放掉supremum的行锁*/
		lock_rec_reset_and_release_wait(page_get_supremum_rec(left_page));
	}

	/*将right_page的所有行锁转移到left page的supremum上，并且作为GAP范围锁(相当于锁升级)*/
	lock_rec_move(page_get_supremum_rec(left_page), page_get_supremum_rec(right_page));
	lock_rec_free_all_from_discard_page(right_page);

	lock_mutex_exit_kernel();
}

/*清空heir的记录行锁，而后继承rec的锁变成heir的GAP范围锁，相当于锁升级*/
void lock_rec_reset_and_inherit_gap_locks(rec_t* heir, rec_t* rec)
{
	mutex_enter(&kernel_mutex);	      		

	lock_rec_reset_and_release_wait(heir);
	lock_rec_inherit_to_gap(heir, rec);

	mutex_exit(&kernel_mutex);	 
}

/*将page的所有行锁转移到heir上*/
void lock_update_discard(rec_t* heir, page_t* page)
{
	rec_t* rec;

	lock_mutex_enter_kernel();
	/*page上没有行锁*/
	if(lock_rec_get_first_on_page(page) == NULL){
		lock_mutex_exit_kernel();
		return ;
	}

	rec = page_get_infimum_rec(page);
	for(;;){
		lock_rec_inherit_to_gap(heir, rec);
		lock_rec_reset_and_release_wait(rec);

		if(rec == page_get_supremum_rec(page))
			break;

		rec = page_rec_get_next(rec);
	}

	/*释放掉所有page上的锁等待*/
	lock_rec_free_all_from_discard_page(page);
}

/*记录添加时的行锁继承*/
void lock_update_insert(rec_t* rec)
{
	lock_mutex_enter_kernel();

	lock_rec_inherit_to_gap(rec, page_rec_get_next(rec));

	lock_mutex_exit_kernel();
}

/*记录删除时的行锁转移*/
void lock_update_delete(rec_t* rec)
{
	lock_mutex_enter_kernel();

	lock_rec_inherit_to_gap(page_rec_get_next(rec), rec);
	lock_rec_reset_and_release_wait(rec);

	lock_mutex_exit_kernel();
}

/*将page的infimum记录行锁转移到rec记录上*/
void lock_rec_restore_from_page_infimum(rec_t* rec, page_t* page)
{
	lock_mutex_enter_kernel();

	lock_rec_move(rec, page_get_infimum_rec(page));

	lock_mutex_exit_kernel();
}

/************************************************************************/



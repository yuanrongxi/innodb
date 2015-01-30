#include "lock0lock.h"
#include "usr0sess.h"
#include "trx0purge.h"

#define LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK	1000000

#define LOCK_RELEASE_KERNEL_INTERVAL		1000

#define LOCK_PAGE_BITMAP_MARGIN				64


ibool lock_print_waits = FALSE;

/*事务行锁全局HASH表,表锁不会放入其中，表锁只通过dict table做关联*/
lock_sys_t* lock_sys = NULL;

/*表事务锁对象定义*/
typedef struct lock_table_struct
{
	dict_table_t* table;			/*字典元数据中的表对象句柄*/
	UT_LIST_NODE_T(lock_t) locks;	/*在表上的事务锁列表*/
}lock_table_t;

typedef struct lock_rec_struct
{
	ulint		space;				/*记录所处的space的ID*/
	ulint		page_no;			/*记录所处的page页号*/
	ulint		n_bits;				/*行锁的bitmap位数，lock_t结构后面会跟一个BUF，长度为n_bits / 8*/
}lock_rec_t;

/*锁对象*/
struct lock_struct
{
	trx_t*			trx;			/*执行事务指针*/
	ulint			type_mode;		/*锁类型和状态，类型有LOCK_ERC和LOCK_TABLE,状态有LOCK_WAIT, LOCK_GAP,强弱：LOCK_X,LOCK_S等*/
	hash_node_t		hash;			/*hash表的对应节点，table lock是无效的*/
	dict_index_t*	index;			/*行锁的行记录索引*/
	UT_LIST_NODE_T(lock_t) trx_locks; /*一个trx_locks的列表前后关系*/
	union{
		lock_table_t	tab_lock;	/*表锁*/
		lock_rec_t		rec_lock;	/*行锁*/
	}un_member;
};

/*死锁标识*/
ibool lock_deadlock_found = FALSE;
/*错误信息缓冲区，5000字节*/
char* lock_latest_err_buf;

/*死锁检测函数*/
static ibool		lock_deadlock_occurs(lock_t* lock, trx_t* trx);
static ibool		lock_deadlock_recursive(trx_t* start, trx_t* trx, lock_t* wait_lock, ulint* cost);

/************************************************************************/
/*kernel_mutex是在srv0srv.h定义的全局内核mutex latch*/
UNIV_INLINE void lock_mutex_enter_kernel()
{
	mutex_enter(&kernel_mutex);
}

UNIV_INLINE void lock_mutex_exit_kernel()
{
	mutex_exit(&kernel_mutex);
}

/*通过聚合索引检查记录是否可以进行一致性锁定读*/
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

/*检查非聚集索引记录是否可以进行一致性读*/
ulint  lock_sec_rec_cons_read_sees(rec_t* rec, dict_index_t* index, read_view_t* view)
{
	dulint	max_trx_id;

	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(page_rec_is_user_rec(rec));

	if(recv_recovery_is_on()) /*检查redo log是否在进行日志恢复*/
		return FALSE;

	/*获得对应page的二级索引最大操作的事务ID*/
	max_trx_id = page_get_max_trx_id(buf_frame_align(rec));
	if(ut_dulint_cmp(max_trx_id, view->up_limit_id) >= 0) /*view中的事务ID大于页中的max trx id,不能进行一致性锁定读*/
		return FALSE;

	return TRUE;
}

/*建立一个系统行锁哈希表对象*/
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

/*获得事务锁的模式(IS, IX, S, X, AINC,NONE)*/
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

/*检查所是否在LOCK_WAIT状态*/
UNIV_INLINE ibool lock_get_wait(lock_t* lock)
{
	ut_ad(lock);
	if(lock->type_mode & LOCK_WAIT) /*锁处于等待状态*/
		return TRUE;

	return FALSE;
}

/*设置事务锁的等待状态LOCK_WAIT*/
UNIV_INLINE void lock_set_lock_and_trx_wait(lock_t* lock, trx_t* trx)
{
	ut_ad(lock);
	ut_ad(trx->wait_lock == NULL);

	trx->wait_lock = lock;
	lock->type_mode = lock->type_mode | LOCK_WAIT;
}

/*清除锁的等待LOCK_WAIT状态*/
UNIV_INLINE void lock_reset_lock_and_trx_wait(lock_t* lock)
{
	ut_ad((lock->trx)->wait_lock == lock);
	ut_ad(lock_get_wait(lock));

	(lock->trx)->wait_lock = NULL;
	lock->type_mode = lock->type_mode & ~LOCK_WAIT;
}

/*判断锁是否是LOCK_GAP范围锁状态*/
UNIV_INLINE ibool lock_rec_get_gap(lock_t* lock)
{
	ut_ad(lock);
	ut_ad(lock_get_type(lock) == LOCK_REC);

	if(lock->type_mode & LOCK_GAP)
		return TRUE;

	return FALSE;
}

/*设置事务锁的记录LOCK_GAP范围锁状态*/
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

/*判断锁的mode1是否比mode2更高强度，一般LOCK_X > LOCK_S > LOCK_IX >= LOCK_IS*/
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

/*假如mode == LOCK_S,返回LOCK_X；mode = LOCK_X返回LOCK_S*/
UNIV_INLINE ulint lock_get_confl_mode(ulint mode)
{
	ut_ad(mode == LOCK_X || mode == LOCK_S);
	if(mode == LOCK_S)
		return LOCK_X;
	
	return LOCK_S;
}

/*判断lock1是否会因为lock2的存在而阻塞事务*/
UNIV_INLINE ibool lock_has_to_wait(lock_t* lock1, lock_t* lock2)
{
	if(lock1->trx != lock2->trx && !lock_mode_compatible(lock_get_mode(lock1), lock_get_mode(lock2)))
		return TRUE;
	return FALSE;
}

/*获得记录行锁所的bitmap长度*/
UNIV_INLINE ulint lock_rec_get_n_bits(lock_t* lock)
{
	return lock->un_member.rec_lock.n_bits;
}

/*获得页第i行的记录行锁*/
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

/*页第i行添加一个lock行锁*/
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

/*获得rec lock bitmap第一个有lock行锁的行序号*/
static ulint lock_rec_find_set_bit(lock_t* lock)
{
	ulint i;
	for(i = 0; i < lock_rec_get_n_bits(lock); i ++){
		if(lock_rec_get_nth_bit(lock, i))
			return i;
	}

	return ULINT_UNDEFINED;
}

/*清除页的第i行的lock行锁状态*/
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

/*获取也得记录同一个页的下一个锁*/
UNIV_INLINE lock_t* lock_rec_get_next_on_page(lock_t* lock)
{
	ulint	space;
	ulint	page_no;

	ut_ad(mutex_own(&kernel_mutex));

	space = lock->un_member.rec_lock.space;
	page_no = lock->un_member.rec_lock.page_no;

	/*在lock_sys的哈希表中查找*/
	for(;;){
		lock = HASH_GET_NEXT(hash, lock);
		if(lock == NULL)
			break;

		/*LOCK还是在同一页中*/
		if(lock->un_member.rec_lock.space == space && lock->un_member.rec_lock.page_no = page_no)
			break;
	}

	return lock;
}

/*获得（space, page_no）指向的page的第一个行锁*/
UNIV_INLINE lock_t* lock_rec_get_first_on_page_addr(ulint space, ulint page_no)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));

	/*lock_sys哈希表中找*/
	lock = HASH_GET_FIRST(lock_sys->rec_hash, lock_rec_hash(space, page_no));
	while(lock){
		if ((lock->un_member.rec_lock.space == space) 
			&& (lock->un_member.rec_lock.page_no == page_no))
			break;

		lock = HASH_GET_NEXT(hash, lock);
	}

	return lock;
}

/*判断（space page_no）指向的页是否有显式行锁*/
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
		/*为什么不是放在外面呢？个人觉得应该放在外面比较好*/
		space = buf_frame_get_space_id(ptr);
		page_no = buf_frame_get_page_no(ptr);

		if(space == lock->un_member.rec_lock.space && page_no == lock->un_member.rec_lock.page_no)
			break;

		lock = HASH_GET_NEXT(hash, lock);
	}

	return lock;
}

/*获得行记录的lock下一个显式行锁*/
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

/*获得rec记录第一个显式行锁*/
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

/*清空lock的bitmap,这个函数不能在事务因为这个行锁阻塞的时候调用，只能在锁建立时初始化用*/
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

/*获得in_lock的前一个的行记录锁,这个记录行的行序号是heap_no*/
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

/*判断事务trx是否持有table的锁比mode更高强度的锁,如果有，返回lock指针*/
UNIV_INLINE lock_t* lock_table_has(trx_t* trx, dict_table_t* table, ulint mode)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));

	/*从后面扫描到前面, 可能trx事务已经有更高强度的锁在这个table上*/
	lock = UT_LIST_GET_LAST(table->locks);
	while(lock != NULL){
		if(lock->trx == trx && lock_mode_stronger_or_eq(lock_get_mode(lock), mode)){
			ut_ad(!lock_get_wait(lock));
			return lock;
		}

		lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, lock);
	}

	return NULL;
}

/*获得一个比mode更高强度的rec行记录锁（显式锁），这个锁必须是trx发起的，并且处于non_gap状态*/
UNIV_INLINE lock_t* lock_rec_has_expl(ulint mode, rec_t* rec, trx_t* trx)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad((mode == LOCK_X) || (mode == LOCK_S));

	lock = lock_rec_get_first(rec);
	while(lock != NULL){
		if(lock->trx == trx && lock_mode_stronger_or_eq(lock_get_mode(lock), mode)
			&& !lock_get_wait(lock) && !(lock_rec_get_gap(lock) || page_rec_is_supremum(rec)))
			return lock;

		lock = lock_rec_get_next(rec, lock);
	}

	return NULL;
}

/*检查是否除trx以外的事务在rec记录上持有比mode更高强度的锁（显式锁）*/
UNIV_INLINE lock_t* lock_rec_other_has_expl_req(ulint mode, ulint gap, ulint wait, rec_t* rec, trx_t* trx)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad((mode == LOCK_X) || (mode == LOCK_S));

	lock = lock_rec_get_first(rec);
	while(lock != NULL){
		if(lock->trx != trx && (gap || !(lock_rec_get_gap(lock) || page_rec_is_supremum(rec))) /*gap锁在supremum，就是+无穷范围*/
			&& (wait || !lock_get_wait(lock)) 
			&& lock_mode_stronger_or_eq(lock_get_mode(lock), mode))
			return lock;

		lock = lock_rec_get_next(rec, lock);
	}

	return NULL;
}

/*在记录所在的page中，查找trx事务发起的type_mode模式的的锁,并且锁所在行的序号必须大于rec的序号*/
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

/*查找rec记录的二级索引是否隐式锁*/
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

	/*查找trx事务发起模式为type_mode的记录行锁，必须是在rec所处的page中的行记录*/
	similar_lock = lock_rec_find_similar_on_page(type_mode, rec, trx);

	/*可以重用similar_lock,一个执行事务在一个行上只会有一个行锁*/
	if(similar_lock != NULL && !somebody_waits && !(type_mode & LOCK_WAIT)){
		lock_rec_set_nth_bit(similar_lock, heap_no); /*在对应行位上加上锁标识，只有目标行是在事务在等待才会添加标识，否则有可能可以直接执行*/
		return similar_lock;
	}

	/*没有对应的行锁，直接创建一个，并进行排队*/
	return lock_rec_create(type_mode, rec, index, trx);
}

/*快速获得行锁，大部分流程都是这样的,没有任何锁在这个行记录上*/
UNIV_INLINE ibool lock_rec_lock_fast(ibool impl, ulint mode, rec_t* rec, dict_index_t* index, que_thr_t* thr)
{
	lock_t*	lock;
	ulint	heap_no;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(mode == LOCK_X || mode == LOCK_S);

	heap_no = rec_get_heap_no(rec);
	lock = lock_rec_get_first_on_page(rec);
	if(lock == NULL){ /*page中没有其他的锁, 创建一个mode类型的锁*/
		if(!impl) /*没有隐式锁，创建一个显式锁在这个行记录上*/
			lock_rec_create(mode, rec, index, thr_get_trx(thr));
		return TRUE;
	}

	/*page的有多个LOCK,不能快速获得锁，进入SLOW模式*/
	if(lock_rec_get_next_on_page(lock))
		return FALSE;

	/*lock的事务与thr中的trx不相同、或者不是行锁、或者lock的记录与rec不相同，直接返回进入SLOW模式*/
	if(lock->trx != thr_get_trx(thr) || lock->type_mode != (mode | LOCK_REC) 
		|| lock_rec_get_n_bits(lock) <= heap_no)
			return FALSE;

	/*有且只有个1行锁(不存在隐身锁)在这个行上，并且这个行锁指向的记录rec,直接认为可以获得锁权*/
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

	/*trx有比mode更加严格的锁模式存在rec行锁(显式锁)，没必要对行上锁*/
	if(lock_rec_has_expl(mode, rec, trx))
		err = DB_SUCCESS;
	else if(lock_rec_other_has_expl_req(confl_mode, 0, LOCK_WAIT, rec, trx)) /*其他事务有更严格的锁(行)在rec行上*/
		err = lock_rec_enqueue_waiting(mode, rec, index, thr); /*创建一个新显式锁并进行等待*/
	else{
		if(!impl) /*增加一个行锁，并加入到锁队列中*/
			lock_rec_add_to_queue(LOCK_REC | mode, rec, index, trx);
		err = DB_SUCCESS;
	}

	return err;
}

/*对记录行上锁请求*/
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

/*检查wait_lock是否还有指向同一行记录并且不兼容的锁,O(n)*/
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

/*获得锁权*/
void lock_grant(lock_t* lock)
{
	ut_ad(mutex_own(&kernel_mutex));

	/*锁的lock wait标识复位*/
	lock_reset_lock_and_trx_wait(lock);

	/*主键自增长锁模式,这种锁在*/
	if(lock_get_mode(lock) == LOCK_AUTO_INC){
		if(lock->trx->auto_inc_lock != NULL)
			fprintf(stderr, "InnoDB: Error: trx already had an AUTO-INC lock!\n");
		
		lock->trx->auto_inc_lock = lock;
	}

	if(lock_print_waits)
		printf("Lock wait for trx %lu ends\n", ut_dulint_get_low(lock->trx->id));

	/*结束事务等待,进行事务执行*/
	trx_end_lock_wait(lock->trx);
}

/*取消正在等待的锁,唤醒锁请求对应的事务*/
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

/*将in_lock从lock_sys中删除， 并唤醒其对应页的一个等待行锁的事务, in_lock可能是一个waiting或者granted状态的锁*/
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

/*遗弃page中所有行记录锁请求*/
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

/*保留同时存在old page的行锁，并重组这些行锁到page对应的行记录上,复杂度O(m * n)*/
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

/*将root page上的行锁转移到new_page*/
void lock_update_root_raise(page_t* new_page, page_t* root)
{
	lock_mutex_enter_kernel();

	lock_rec_move(page_get_supremum_rec(new_page), page_get_supremum_rec(root));

	lock_mutex_exit_kernel();
}

void lock_update_copy_and_discard(page_t* new_page, page_t* page)
{
	lock_mutex_enter_kernel();

	/*将page的锁全部转移到new_page的supremum上，相当于GAP范围升级*/
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

/*清空heir的记录行锁，而后继承rec的锁变成heir的GAP范围锁*/
void lock_rec_reset_and_inherit_gap_locks(rec_t* heir, rec_t* rec)
{
	mutex_enter(&kernel_mutex);	      		

	lock_rec_reset_and_release_wait(heir);
	lock_rec_inherit_to_gap(heir, rec);

	mutex_exit(&kernel_mutex);	 
}

/*将page的所有行锁转移到heir行上*/
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

	/*遗弃掉所有page上的锁等待*/
	lock_rec_free_all_from_discard_page(page);
}

/*记录添加或修改时的行锁，是个GAP范围锁，*/
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

/*将rec全部移到infimum上*/
void lock_rec_store_on_page_infimum(rec_t* rec)
{
	page_t* page;
	page = buf_frame_align(rec);

	lock_mutex_enter_kernel();
	lock_rec_move(page_get_infimum_rec(page), rec);
	lock_mutex_exit_kernel();
}

/*将page的infimum记录行锁转移到rec记录上*/
void lock_rec_restore_from_page_infimum(rec_t* rec, page_t* page)
{
	lock_mutex_enter_kernel();

	lock_rec_move(rec, page_get_infimum_rec(page));

	lock_mutex_exit_kernel();
}

/*检查一个锁请求是否会造成事务死锁,是一个递归检测过程*/
static ibool lock_deadlock_occurs(lock_t* lock, trx_t* trx)
{
	dict_table_t*	table;
	dict_index_t*	index;
	trx_t*		mark_trx;
	ibool		ret;
	ulint		cost	= 0;
	char*		err_buf;

	ut_ad(trx && lock);
	ut_ad(mutex_own(&kernel_mutex));

	/*初始化所有事务的deadlock_mark*/
	mark_trx = UT_LIST_GET_FIRST(trx_list, mark_trx);
	while(mark_trx){
		mark_trx->deadlock_mark = 0;
		mark_trx = UT_LIST_GET_NEXT(trx_list, mark_trx);
	}
	/*对死锁进行检测*/
	ret = lock_deadlock_recursive(trx, trx, lock, &cost);
	if(ret){ /*构建死锁信息*/
		if(lock_get_type(lock) == LOCK_TABLE){
			table = lock->un_member.tab_lock.table;
			index = NULL;
		}
		else{
			index = lock->index;
			table = index->table;
		}

		lock_deadlock_found = TRUE;

		err_buf = lock_latest_err_buf + sizeof(lock_latest_err_buf);
		err_buf += sprintf(err_buf, "*** (2) WAITING FOR THIS LOCK TO BE GRANTED:\n");
		ut_a(err_buf <= lock_latest_err_buf + 4000);

		if(lock_get_type(lock) == LOCK_REC){
			lock_rec_print(err_buf, lock);
			err_buf += strlen(err_buf);
		}
		else{
			lock_table_print(err_buf, lock);
			err_buf += strlen(err_buf);
		}

		ut_a(err_buf <= lock_latest_err_buf + 4000);
		err_buf += sprintf(err_buf, "*** WE ROLL BACK TRANSACTION (2)\n");
		ut_a(strlen(lock_latest_err_buf) < 4100);
	}

	return ret;
}

static ibool lock_deadlock_recursive(trx_t* start, trx_t* trx, lock_t* wait_lock, ulint* cost)
{
	lock_t*	lock;
	ulint	bit_no;
	trx_t*	lock_trx;
	char*	err_buf;

	ut_a(trx && start && wait_lock);
	ut_ad(mutex_own(&kernel_mutex));

	if(trx->deadlock_mark == 1)
		return TRUE;

	*cost = *cost + 1;
	if(*cost > LOCK_MAX_N_STEPS_IN_DEADLOCK_CHECK)
		return TRUE;

	lock = wait_lock;
	if(lock_get_type(wait_lock) == LOCK_REC){
		bit_no = lock_rec_find_set_bit(wait_lock);
		ut_a(bit_no != ULINT_UNDEFINED);
	}

	for(;;){
		if(lock_get_type(lock) == LOCK_TABLE){ /*查找上一个表锁*/
			lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, lock);
		}
		else{ /*查找记录行的上一个行锁*/
			ut_ad(lock_get_type(lock) == LOCK_REC);
			lock = lock_rec_get_prev(lock, bit_no);
		}

		if(lock == NULL){
			trx->deadlock_mark = 1;
			return FALSE;
		}

		if(lock_has_to_wait(wait_lock, lock)){
			lock_trx = lock->trx;

			if(lock_trx == start){ /*已经构成死锁环*/
				err_buf = lock_latest_err_buf;

				ut_sprintf_timestamp(err_buf);
				err_buf += strlen(err_buf);

				err_buf += sprintf(err_buf,
					"  LATEST DETECTED DEADLOCK:\n"
					"*** (1) TRANSACTION:\n");

				trx_print(err_buf, wait_lock->trx);
				err_buf += strlen(err_buf);

				err_buf += sprintf(err_buf,
					"*** (1) WAITING FOR THIS LOCK TO BE GRANTED:\n");

				ut_a(err_buf <= lock_latest_err_buf + 4000);

				if (lock_get_type(wait_lock) == LOCK_REC) {
					lock_rec_print(err_buf, wait_lock);
					err_buf += strlen(err_buf);
				} else {
					lock_table_print(err_buf, wait_lock);
					err_buf += strlen(err_buf);
				}

				ut_a(err_buf <= lock_latest_err_buf + 4000);
				err_buf += sprintf(err_buf,
					"*** (2) TRANSACTION:\n");

				trx_print(err_buf, lock->trx);
				err_buf += strlen(err_buf);

				err_buf += sprintf(err_buf,
					"*** (2) HOLDS THE LOCK(S):\n");

				ut_a(err_buf <= lock_latest_err_buf + 4000);

				if (lock_get_type(lock) == LOCK_REC) {
					lock_rec_print(err_buf, lock);
					err_buf += strlen(err_buf);
				} else {
					lock_table_print(err_buf, lock);
					err_buf += strlen(err_buf);
				}

				ut_a(err_buf <= lock_latest_err_buf + 4000);

				if (lock_print_waits) {
					printf("Deadlock detected\n");
				}

				return(TRUE);
			}

			/*如果是lock_trx等待状态，进行递归判断*/
			if(lock_trx->que_state == TRX_QUE_LOCK_WAIT){
				if(lock_deadlock_recursive(start, lock_trx, lock_trx->wait_lock, cost))
					return TRUE;
			}
		}
	}
}

UNIV_INLINE lock_t* lock_table_create(dict_table_t* table, ulint type_mode, trx_t* trx)
{
	lock_t*	lock;

	ut_ad(table && trx);
	ut_ad(mutex_own(&kernel_mutex));

	if(type_mode == LOCK_AUTO_INC){ /*直接将表的自增锁返回*/
		lock = table->auto_inc_lock;
		ut_a(trx->auto_inc_lock);
		trx->auto_inc_lock = lock;
	}
	else /*在trx事务的分配heap分配一个lock*/
		lock = mem_heap_alloc(trx->lock_heap, sizeof(lock_t));

	if(lock == NULL)
		return NULL;

	/*加入到事务的锁列表中*/
	UT_LIST_ADD_LAST(trx_locks, trx->trx_locks, lock);
	
	lock->type_mode = type_mode | LOCK_TABLE;
	lock->trx = trx;

	lock->un_member.tab_lock.table = table;
	UT_LIST_ADD_LAST(un_member.tab_lock.locks, table->locks, lock);

	if(type_mode & LOCK_WAIT)
		lock_set_lock_and_trx_wait(lock, trx);

	/*todo:表锁是不会放入全局的lock_sys当中*/
	return lock;
}

/*移除table_lock*/
UNIV_INLINE void lock_table_remove_low(lock_t* lock)
{
	dict_table_t*	table;
	trx_t*			trx;

	ut_ad(mutex_own(&kernel_mutex));

	table = lock->un_member.table_lock.table;
	trx = lock->trx;

	/*自增长锁*/
	if(lock == trx->auto_inc_lock)
		trx->auto_inc_lock = NULL;

	/*从trx事务移除lock*/
	UT_LIST_REMOVE(trx_locks, trx->trx_locks, lock);
	/*从table中移除locks*/
	UT_LIST_REMOVE(un_member.tab_lock.locks, table->locks, lock);
}

ulint lock_table_enqueue_waiting(ulint mode, dict_table_t* table, que_thr_t* thr)
{
	lock_t*	lock;
	trx_t*	trx;

	ut_ad(mutex_own(&kernel_mutex));

	if(que_thr_stop(thr)){
		ut_a(0);
		return DB_QUE_THR_SUSPENDED;
	}

	/*获得thr正在执行的事务*/
	trx = thr_get_trx(thr);
	if(trx->dict_operation){
		ut_print_timestamp(stderr);
		fprintf(stderr, "  InnoDB: Error: a table lock wait happens in a dictionary operation!\n"
			"InnoDB: Table name %s. Send a bug report to mysql@lists.mysql.com\n", table->name);
	}

	/*建立一个表锁*/
	lock = lock_table_create(table, mode | LOCK_WAIT, trx);
	if(lock_deadlock_occurs(lock, trx)){ /*死锁了！！*/
		lock_reset_lock_and_trx_wait(lock);
		lock_table_remove_low(lock);

		return(DB_DEADLOCK);
	}

	trx->que_state = TRX_QUE_LOCK_WAIT;
	trx->wait_started = time(NULL);

	ut_a(que_thr_stop(thr));

	return DB_LOCK_WAIT;
}

/*检查表持有的表锁是否和mode模式排斥？*/
UNIV_INLINE ibool lock_table_other_has_incompatible(trx_t* trx, ulint wait, dict_table_t* table, ulint mode)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));

	lock = UT_LIST_GET_LAST(table->locks);
	while(lock != NULL){
		if(lock->trx == trx && (!lock_mode_compatible(lock_get_mode(lock), mode))
			&& (wait || !lock_get_wait(lock)))
			return TRUE;

		lock = UT_LIST_GET_PREV(un_member.tab_lock.locks, lock);
	}

	return FALSE; 
}

ulint lock_table(ulint flags, dict_table_t* table, ulint mode, que_thr_t* thr)
{
	trx_t*	trx;
	ulint	err;

	ut_ad(table && thr);

	if(flags & BTR_NO_LOCKING_FLAG)
		return DB_SUCCESS;

	trx = thr_get_trx(thr);

	lock_mutex_enter_kernel();

	/*判断是否有更严的锁*/
	if(lock_table_has(trx, table, mode)){
		lock_mutex_exit_kernel();
		return DB_SUCCESS;
	}

	/*检查锁是否排斥*/
	if(lock_table_other_has_incompatible(trx, LOCK_WAIT, table, mode)){ /*锁排斥，必须进行排队*/
		err = lock_table_enqueue_waiting(mode, table, thr);
		lock_mutex_exit_kernel();

		return err;
	}
	/*进行锁创建并LOCK_WAIT*/
	lock_table_create(table, mode, trx);

	lock_mutex_exit_kernel();

	return DB_SUCCESS;
}
/*判断table是有表锁请求*/
ibool lock_is_on_table(dict_table_t* table)
{
	ibool	ret;

	ut_ad(table);

	lock_mutex_enter_kernel();
	
	if(UT_LIST_GET_LAST(table->locks) != NULL)
		ret = TRUE;
	else
		ret = FALSE;

	lock_mutex_exit_kernel();

	return ret;
}

/*判断wait_lock是否需要在队列中等待锁请求*/
static ibool lock_table_has_to_wait_in_queue(lock_t* wait_lock)
{
	dict_table_t*	table;
	lock_t*			lock;

	ut_ad(lock_get_wait(wait_lock));

	table = wait_lock->un_member.tab_lock.table;

	lock = UT_LIST_GET_FIRST(table->locks);
	while(lock != wait_lock){
		if(lock_has_to_wait(wait_lock, lock))
			return TRUE;

		lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
	}

	return TRUE;
}

/*对in_lock进行移除，并激活可以激活的所有的表锁事务*/
void lock_table_dequeue(lock_t* in_lock)
{
	lock_t* lock;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(lock_get_type(in_lock) == LOCK_TABLE);

	/*获得in_lock下一个表锁句柄*/
	lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, in_lock);
	/*移除in_lock*/
	lock_table_remove_low(in_lock);
	/*激活所有可以激活的锁的事务*/
	while(lock != NULL){
		if(lock_get_wait(lock) && !lock_table_has_to_wait_in_queue(lock))
			lock_grant(lock);

		lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
	}
}

/*释放一个自增长锁*/
void lock_table_unlock_auto_inc(trx_t* trx)
{
	if(trx->auto_inc_lock){
		mutex_enter(&kernel_mutex);

		lock_table_dequeue(trx->auto_inc_lock);

		mutex_exit(&kernel_mutex);
	}
}

/*释放一个事务的所有锁请求*/
void lock_release_off_kernel(trx_id* trx)
{
	ulint	count;
	lock_t*	lock;

	ut_ad(mutex_own(&kernel_mutex));

	lock = UT_LIST_GET_LAST(trx->trx_locks);
	count = 0;
	while(lock != NULL){
		count ++;

		/*激活下一个记录行或者表锁*/
		if(lock_get_type(lock) == LOCK_REC)
			lock_rec_dequeue_from_page(lock);
		else{
			ut_ad(lock_get_type(lock) == LOCK_TABLE);
			lock_table_dequeue(lock);
		}

		/*释放一次kernel latch,以便其他线程进行并发，防止lock_release_off_kernel函数长时间堵塞*/
		if(count == LOCK_RELEASE_KERNEL_INTERVAL){
			lock_mutex_exit_kernel();
			lock_mutex_enter_kernel();
			count = 0;
		}

		lock = UT_LIST_GET_LAST(trx->trx_locks);
	}

	/*释放事务对应的lock分配堆*/
	mem_heap_empty(trx->lock_heap);

	ut_a(trx->auto_inc_lock == NULL);
}

/*取消lock，并激活下一个对应的lock*/
void lock_cancel_waiting_and_release(lock_t* lock)
{
	ut_ad(mutex_own(&kernel_mutex));

	/*激活等待本锁的事务*/
	if(lock_get_type(lock) == LOCK_REC)
		lock_rec_dequeue_from_page(lock);
	else{
		ut_ad(lock_get_type(lock) == LOCK_TABLE);
		lock_table_dequeue(lock);
	}

	/*取消lock的等待位*/
	lock_reset_lock_and_trx_wait(lock);
	/*lock的事务继续执行*/
	trx_end_lock_wait(lock->trx);
}

/*复位事务trx所有的锁，主要是从等待队列中删除*/
static void lock_reset_all_on_table_for_trx(dict_table_t* table, trx_t* trx)
{
	lock_t*	lock;
	lock_t*	prev_lock;

	ut_ad(mutex_own(&kernel_mutex));

	lock = UT_LIST_GET_LAST(trx->trx_locks);
	while(lock != NULL){
		prev_lock = UT_LIST_GET_PREV(trx_locks, lock);

		if(lock_get_type(lock) == LOCK_REC && lock->index->table == table){
			ut_a(!lock_get_wait(lock));
			lock_rec_discard(lock);
		}
		else if(lock_get_type(lock) == LOCK_TABLE && lock->un_member.tab_lock.table == table){
			ut_a(!lock_get_wait(lock));
			lock_table_remove_low(lock);
		}

		lock = prev_lock;
	}
}

/*复位table的表锁请求*/
void lock_reset_all_on_table(dict_table_t* table)
{
	lock_t* lock;

	mutex_enter(&kernel_mutex);

	lock = UT_LIST_GET_FIRST(table->locks);

	while(lock){
		ut_a(!lock_get_wait(lock));

		/*复位lock->trx的表锁请求*/
		lock_reset_all_on_table_for_trx(table, lock->trx);

		lock = UT_LIST_GET_FIRST(table->locks);
	}

	mutex_exit(&kernel_mutex);
}

/**********************VALIDATION AND DEBUGGING *************************/
void
lock_table_print(char* buf, lock_t* lock)
{
	ut_ad(mutex_own(&kernel_mutex));
	ut_a(lock_get_type(lock) == LOCK_TABLE);

	buf += sprintf(buf, "TABLE LOCK table %s trx id %lu %lu",
		lock->un_member.tab_lock.table->name, (lock->trx)->id.high, (lock->trx)->id.low);

	if (lock_get_mode(lock) == LOCK_S)
		buf += sprintf(buf, " lock mode S");
	else if (lock_get_mode(lock) == LOCK_X)
		buf += sprintf(buf, " lock_mode X");
	else if (lock_get_mode(lock) == LOCK_IS)
		buf += sprintf(buf, " lock_mode IS");
	else if (lock_get_mode(lock) == LOCK_IX)
		buf += sprintf(buf, " lock_mode IX");
	else if (lock_get_mode(lock) == LOCK_AUTO_INC)
		buf += sprintf(buf, " lock_mode AUTO-INC");
	else
		buf += sprintf(buf," unknown lock_mode %lu", lock_get_mode(lock));

	if (lock_get_wait(lock))
		buf += sprintf(buf, " waiting");

	buf += sprintf(buf, "\n");
}

void
lock_rec_print(char* buf,lock_t* lock)
{
	page_t*	page;
	ulint	space;
	ulint	page_no;
	ulint	i;
	ulint	count	= 0;
	char*	buf_start	= buf;
	mtr_t	mtr;

	ut_ad(mutex_own(&kernel_mutex));
	ut_a(lock_get_type(lock) == LOCK_REC);

	space = lock->un_member.rec_lock.space;
 	page_no = lock->un_member.rec_lock.page_no;

	buf += sprintf(buf, "RECORD LOCKS space id %lu page no %lu n bits %lu",
		    space, page_no, lock_rec_get_n_bits(lock));

	buf += sprintf(buf, " table %s index %s trx id %lu %lu",
		lock->index->table->name, lock->index->name,
		(lock->trx)->id.high, (lock->trx)->id.low);

	if (lock_get_mode(lock) == LOCK_S) {
		buf += sprintf(buf, " lock mode S");
	} else if (lock_get_mode(lock) == LOCK_X) {
		buf += sprintf(buf, " lock_mode X");
	} else {
		ut_error;
	}

	if (lock_rec_get_gap(lock)) {
		buf += sprintf(buf, " gap type lock");
	}

	if (lock_get_wait(lock)) {
		buf += sprintf(buf, " waiting");
	}

	mtr_start(&mtr);

	buf += sprintf(buf, "\n");

	/* If the page is not in the buffer pool, we cannot load it
	because we have the kernel mutex and ibuf operations would
	break the latching order */
	
	page = buf_page_get_gen(space, page_no, RW_NO_LATCH, NULL, BUF_GET_IF_IN_POOL, IB__FILE__, __LINE__, &mtr);
	if (page) {
		page = buf_page_get_nowait(space, page_no, RW_S_LATCH, &mtr);
	}
				
	if (page) {
		buf_page_dbg_add_level(page, SYNC_NO_ORDER_CHECK);
	}

	for (i = 0; i < lock_rec_get_n_bits(lock); i++) {
		if (buf - buf_start > 300) {
			buf += sprintf(buf,"Suppressing further record lock prints for this page\n");
			mtr_commit(&mtr);

			return;
		}
	
		if (lock_rec_get_nth_bit(lock, i)) {
			buf += sprintf(buf, "Record lock, heap no %lu ", i);

			if (page) {
				buf += rec_sprintf(buf, 120, page_find_rec_with_heap_no(page, i));
				*buf = '\0';
			}

			buf += sprintf(buf, "\n");
			count++;
		}
	}

	mtr_commit(&mtr);
}

/*统计记录行锁的个数*/
static ulint lock_get_n_rec_locks()
{
	lock_t*	lock;
	ulint n_locks = 0;
	ulint i;

	ut_ad(mutex_own(&kernel_mutex));

	for(i = 0; i < hash_get_n_cells(lock_sys->rec_hash); i ++){
		lock = HASH_GET_FIRST(lock_sys->rec_hash, i);
		while(lock){
			n_locks ++;
			lock = HASH_GET_NEXT(hash, lock);
		}
	}

	return n_locks;
}

void lock_print_info(char*	buf, char*	buf_end)
{
	lock_t*	lock;
	trx_t*	trx;
	ulint	space;
	ulint	page_no;
	page_t*	page;
	ibool	load_page_first = TRUE;
	ulint	nth_trx		= 0;
	ulint	nth_lock	= 0;
	ulint	i;
	mtr_t	mtr;

	if (buf_end - buf < 600) {
		sprintf(buf, "... output truncated!\n");
		return;
	}

	buf += sprintf(buf, "Trx id counter %lu %lu\n", 
		ut_dulint_get_high(trx_sys->max_trx_id),
		ut_dulint_get_low(trx_sys->max_trx_id));

	buf += sprintf(buf,
	"Purge done for trx's n:o < %lu %lu undo n:o < %lu %lu\n",
		ut_dulint_get_high(purge_sys->purge_trx_no),
		ut_dulint_get_low(purge_sys->purge_trx_no),
		ut_dulint_get_high(purge_sys->purge_undo_no),
		ut_dulint_get_low(purge_sys->purge_undo_no));
	
	lock_mutex_enter_kernel();

	buf += sprintf(buf,"Total number of lock structs in row lock hash table %lu\n", lock_get_n_rec_locks());
	if (lock_deadlock_found) {

		if ((ulint)(buf_end - buf)
			< 100 + strlen(lock_latest_err_buf)) {

			lock_mutex_exit_kernel();
			sprintf(buf, "... output truncated!\n");

			return;
		}

		buf += sprintf(buf, "%s", lock_latest_err_buf);
	}

	if (buf_end - buf < 600) {
		lock_mutex_exit_kernel();
		sprintf(buf, "... output truncated!\n");

		return;
	}

	buf += sprintf(buf, "LIST OF TRANSACTIONS FOR EACH SESSION:\n");

	/* First print info on non-active transactions */

	trx = UT_LIST_GET_FIRST(trx_sys->mysql_trx_list);

	while (trx) {
		if (buf_end - buf < 900) {
			lock_mutex_exit_kernel();
			sprintf(buf, "... output truncated!\n");

			return;
		}

		if (trx->conc_state == TRX_NOT_STARTED) {
		    buf += sprintf(buf, "---");
			trx_print(buf, trx);

			buf += strlen(buf);
		}
			
		trx = UT_LIST_GET_NEXT(mysql_trx_list, trx);
	}

loop:
	trx = UT_LIST_GET_FIRST(trx_sys->trx_list);

	i = 0;

	/* Since we temporarily release the kernel mutex when
	reading a database page in below, variable trx may be
	obsolete now and we must loop through the trx list to
	get probably the same trx, or some other trx. */
	
	while (trx && (i < nth_trx)) {
		trx = UT_LIST_GET_NEXT(trx_list, trx);
		i++;
	}

	if (trx == NULL) {
		lock_mutex_exit_kernel();
		return;
	}

	if (buf_end - buf < 900) {
		lock_mutex_exit_kernel();
		sprintf(buf, "... output truncated!\n");

		return;
	}

	if (nth_lock == 0) {
	        buf += sprintf(buf, "---");
		trx_print(buf, trx);

		buf += strlen(buf);
		
		if (buf_end - buf < 500) {
			lock_mutex_exit_kernel();
			sprintf(buf, "... output truncated!\n");

			return;
		}
		
	        if (trx->read_view) {
	  	        buf += sprintf(buf,
       "Trx read view will not see trx with id >= %lu %lu, sees < %lu %lu\n",
		       	ut_dulint_get_high(trx->read_view->low_limit_id),
       			ut_dulint_get_low(trx->read_view->low_limit_id),
       			ut_dulint_get_high(trx->read_view->up_limit_id),
       			ut_dulint_get_low(trx->read_view->up_limit_id));
	        }

		if (trx->que_state == TRX_QUE_LOCK_WAIT) {
			buf += sprintf(buf,
 "------- TRX HAS BEEN WAITING %lu SEC FOR THIS LOCK TO BE GRANTED:\n",
		   (ulint)difftime(time(NULL), trx->wait_started));

			if (lock_get_type(trx->wait_lock) == LOCK_REC) {
				lock_rec_print(buf, trx->wait_lock);
			} else {
				lock_table_print(buf, trx->wait_lock);
			}

			buf += strlen(buf);
			buf += sprintf(buf,
			"------------------\n");
		}
	}

	if (!srv_print_innodb_lock_monitor) {
	  	nth_trx++;
	  	goto loop;
	}

	i = 0;

	/* Look at the note about the trx loop above why we loop here:
	lock may be an obsolete pointer now. */
	
	lock = UT_LIST_GET_FIRST(trx->trx_locks);
		
	while (lock && (i < nth_lock)) {
		lock = UT_LIST_GET_NEXT(trx_locks, lock);
		i++;
	}

	if (lock == NULL) {
		nth_trx++;
		nth_lock = 0;

		goto loop;
	}

	if (buf_end - buf < 500) {
		lock_mutex_exit_kernel();
		sprintf(buf, "... output truncated!\n");

		return;
	}

	if (lock_get_type(lock) == LOCK_REC) {
		space = lock->un_member.rec_lock.space;
 		page_no = lock->un_member.rec_lock.page_no;

 		if (load_page_first) {
			lock_mutex_exit_kernel();

			mtr_start(&mtr);
			
			page = buf_page_get_with_no_latch(space, page_no, &mtr);

			mtr_commit(&mtr);

			load_page_first = FALSE;

			lock_mutex_enter_kernel();

			goto loop;
		}
		
		lock_rec_print(buf, lock);
	} else {
		ut_ad(lock_get_type(lock) == LOCK_TABLE);
		lock_table_print(buf, lock);
	}

	buf += strlen(buf);
	
	load_page_first = TRUE;

	nth_lock++;

	if (nth_lock >= 10) {
		buf += sprintf(buf, "10 LOCKS PRINTED FOR THIS TRX: SUPPRESSING FURTHER PRINTS\n");
	
		nth_trx++;
		nth_lock = 0;

		goto loop;
	}

	goto loop;
}

/*判断一个表锁的合法性*/
ibool lock_table_queue_validate(dict_table_t* table)
{
	lock_t*	lock;
	ibool	is_waiting;

	ut_ad(mutex_own(&kernel_mutex));

	is_waiting = FALSE;

	lock = UT_LIST_GET_FIRST(table->locks);

	while (lock) {
		ut_a(((lock->trx)->conc_state == TRX_ACTIVE) || ((lock->trx)->conc_state == TRX_COMMITTED_IN_MEMORY));

		if (!lock_get_wait(lock)) { /*如果锁获得执行权*/
			ut_a(!is_waiting);
			ut_a(!lock_table_other_has_incompatible(lock->trx, 0, table, lock_get_mode(lock)));
		} else {
			is_waiting = TRUE;
			ut_a(lock_table_has_to_wait_in_queue(lock));
		}

		lock = UT_LIST_GET_NEXT(un_member.tab_lock.locks, lock);
	}

	return(TRUE);
}

ibool lock_rec_queue_validate(rec_t* rec, dict_index_t* index)	
{
	trx_t*	impl_trx;	
	lock_t*	lock;
	ibool	is_waiting;
	
	ut_a(rec);

	lock_mutex_enter_kernel();

	/*supremum/infimum的行锁判断*/
	if (page_rec_is_supremum(rec) || page_rec_is_infimum(rec)) {
		lock = lock_rec_get_first(rec);

		while (lock) {
			ut_a(lock->trx->conc_state == TRX_ACTIVE || lock->trx->conc_state == TRX_COMMITTED_IN_MEMORY);
			ut_a(trx_in_trx_list(lock->trx));
			
			/*锁处于LOCK_WAIT状态下，必须在等待队列中*/
			if (lock_get_wait(lock))
				ut_a(lock_rec_has_to_wait_in_queue(lock));

			/*lock的index必须是index*/
			if (index)
				ut_a(lock->index == index);

			lock = lock_rec_get_next(rec, lock);
		}

		lock_mutex_exit_kernel();

	    return(TRUE);
	}

	if (index && (index->type & DICT_CLUSTERED)) {
		impl_trx = lock_clust_rec_some_has_impl(rec, index);

		if (impl_trx && lock_rec_other_has_expl_req(LOCK_S, 0, LOCK_WAIT, rec, impl_trx))
			ut_a(lock_rec_has_expl(LOCK_X, rec, impl_trx));
	}

	if (index && !(index->type & DICT_CLUSTERED)) {
		
		/* The kernel mutex may get released temporarily in the
		next function call: we have to release lock table mutex
		to obey the latching order */
		
		impl_trx = lock_sec_rec_some_has_impl_off_kernel(rec, index);

		if (impl_trx && lock_rec_other_has_expl_req(LOCK_S, 0,
						LOCK_WAIT, rec, impl_trx)) {

			ut_a(lock_rec_has_expl(LOCK_X, rec, impl_trx));
		}
	}

	is_waiting = FALSE;

	lock = lock_rec_get_first(rec);

	while (lock) {
		ut_a(lock->trx->conc_state == TRX_ACTIVE
		     || lock->trx->conc_state == TRX_COMMITTED_IN_MEMORY);
		ut_a(trx_in_trx_list(lock->trx));
	
		if (index) {
			ut_a(lock->index == index);
		}

		if (!lock_rec_get_gap(lock) && !lock_get_wait(lock)) {

			ut_a(!is_waiting);
		
			if (lock_get_mode(lock) == LOCK_S) {
				ut_a(!lock_rec_other_has_expl_req(LOCK_X,0, 0, rec, lock->trx));
			} else {
				ut_a(!lock_rec_other_has_expl_req(LOCK_S,0, 0, rec, lock->trx));
			}

		} else if (lock_get_wait(lock) && !lock_rec_get_gap(lock)) {
			is_waiting = TRUE;
			ut_a(lock_rec_has_to_wait_in_queue(lock));
		}

		lock = lock_rec_get_next(rec, lock);
	}

	lock_mutex_exit_kernel();

	return(TRUE);
}

ibool lock_rec_validate_page(ulint space, ulint	page_no)
{
	dict_index_t*	index;
	page_t*	page;
	lock_t*	lock;
	rec_t*	rec;
	ulint	nth_lock	= 0;
	ulint	nth_bit		= 0;
	ulint	i;
	mtr_t	mtr;

	ut_ad(!mutex_own(&kernel_mutex));

	mtr_start(&mtr);

	page = buf_page_get(space, page_no, RW_X_LATCH, &mtr);
	buf_page_dbg_add_level(page, SYNC_NO_ORDER_CHECK);

	lock_mutex_enter_kernel();

loop:	
	lock = lock_rec_get_first_on_page_addr(space, page_no);
	if (lock == NULL)
		goto function_exit;

	for (i = 0; i < nth_lock; i++) {
		lock = lock_rec_get_next_on_page(lock);

		if (!lock) 
			goto function_exit;
	}

	ut_a(trx_in_trx_list(lock->trx));
	ut_a(lock->trx->conc_state == TRX_ACTIVE
		|| lock->trx->conc_state == TRX_COMMITTED_IN_MEMORY);

	for (i = nth_bit; i < lock_rec_get_n_bits(lock); i++) {
		if (i == 1 || lock_rec_get_nth_bit(lock, i)) {

			index = lock->index;
			rec = page_find_rec_with_heap_no(page, i);

			printf("Validating %lu %lu\n", space, page_no);

			lock_mutex_exit_kernel();

			lock_rec_queue_validate(rec, index);

			lock_mutex_enter_kernel();

			nth_bit = i + 1;

			goto loop;
		}
	}

	nth_bit = 0;
	nth_lock++;

	goto loop;

function_exit:
	lock_mutex_exit_kernel();
	mtr_commit(&mtr);

	return(TRUE);
}

/*对事务锁的整体校验*/
ibool lock_validate(void)
{
	lock_t*	lock;
	trx_t*	trx;
	dulint	limit;
	ulint	space;
	ulint	page_no;
	ulint	i;

	lock_mutex_enter_kernel();

	trx = UT_LIST_GET_FIRST(trx_sys->trx_list);

	while (trx) {
		lock = UT_LIST_GET_FIRST(trx->trx_locks);

		while (lock) {
			if (lock_get_type(lock) == LOCK_TABLE)
				lock_table_queue_validate(lock->un_member.tab_lock.table);

			lock = UT_LIST_GET_NEXT(trx_locks, lock);
		}

		trx = UT_LIST_GET_NEXT(trx_list, trx);
	}

	for (i = 0; i < hash_get_n_cells(lock_sys->rec_hash); i++) {

		limit = ut_dulint_zero;

		for (;;) {
			lock = HASH_GET_FIRST(lock_sys->rec_hash, i);

			while (lock) {
				ut_a(trx_in_trx_list(lock->trx));

				space = lock->un_member.rec_lock.space;
				page_no = lock->un_member.rec_lock.page_no;

				if (ut_dulint_cmp(ut_dulint_create(space, page_no),limit) >= 0) {
					break;
				}

				lock = HASH_GET_NEXT(hash, lock);
			}

			if (!lock) {
				break;
			}

			lock_mutex_exit_kernel();

			lock_rec_validate_page(space, page_no);

			lock_mutex_enter_kernel();

			limit = ut_dulint_create(space, page_no + 1);
		}
	}

	lock_mutex_exit_kernel();

	return(TRUE);
}

ulint lock_rec_insert_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, que_thr_t* thr, ibool inherit)
{
	rec_t*	next_rec;
	trx_t*	trx;
	lock_t*	lock;
	ulint	err;

	if(flags && BTR_NO_LOCKING_FLAG)
		return DB_SUCCESS;

	ut_ad(rec);

	trx = thr_get_trx(thr);
	next_rec = page_rec_get_next(rec);

	*thr_get_trx = FALSE;

	lock_mutex_enter_kernel();

	ut_ad(lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));

	lock = lock_rec_get_first(next_rec);
	if(lock == NULL){ /*行记录可以操作*/
		lock_mutex_exit_kernel();
		
		/*更新page最后执行的事务ID*/
		if(!(index->type) & DICT_CLUSTERED)
			page_update_max_trx_id(buf_frame_align(rec), thr_get_trx(thr)->id);

		return DB_SUCCESS;
	}

	*inherit = TRUE;

	/*next_rec上有比LOCK_S更严格的行锁，并且不是trx发起的,insert操作必须插入一个LOCK_X来支持插入操作*/
	if(lock_rec_other_has_expl_req(LOCK_S, LOCK_GAP, LOCK_WAIT, next_rec, trx))
		err = lock_rec_enqueue_waiting(LOCK_X | LOCK_GAP, next_rec, index, thr); /*插入一个LOCK_X独占锁来进行操作*/
	else
		err = DB_SUCCESS;

	lock_mutex_exit_kernel();

	/*更新最近操作page的事务ID*/
	if(!(index->type & DICT_CLUSTERED) && (err == DB_SUCCESS)){
		page_update_max_trx_id(buf_frame_align(rec), thr_get_trx(thr)->id);
	}

	ut_ad(lock_rec_queue_validate(next_rec, index));

	return err;
}

/*将隐式锁转换成一个显示的LOCK_X*/
static void lock_rec_convert_impl_to_expl(rec_t* rec, dict_index_t* index)
{
	trx_t*	impl_trx;

	ut_ad(mutex_own(&kernel_mutex));
	ut_ad(page_rec_is_user_rec(rec));

	if(index->type & DICT_CLUSTERED) /*聚集索引上的隐式锁的事务获取*/
		impl_trx = lock_clust_rec_some_has_impl(rec, index);
	else /*辅助索引上的隐式索引的事务获取，这个过程非常复杂！！！*/
		impl_trx = lock_sec_rec_some_has_impl_off_kernel(rec, index);

	if(impl_trx){
		if(lock_rec_has_expl(LOCK_X, rec, impl_trx) == NULL) /*impl_trx没有一个rec记录行锁比LOCK_X更严格*/
			lock_rec_add_to_queue(LOCK_REC | LOCK_X, rec, index, impl_trx); /*增加一个LOCK_X到这个行上*/
	}
}

/*假如一个事务需要对记录REC进行修改，检查主索引是否有隐式锁，如果有转换成显示锁，并且尝试获得锁权执行事务*/
ulint lock_clust_rec_modify_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, que_thr_t* thr)
{
	trx_t*	trx;
	ulint	err;

	if(flags & BTR_NO_LOCKING_FLAG)
		return DB_SUCCESS;

	ut_ad(index->type & DICT_CLUSTERED);

	trx = thr_get_trx(thr);

	lock_mutex_enter_kernel();
	ut_ad(lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
	/*如果聚集索引上存在隐式锁，转换成显示锁*/
	lock_rec_convert_impl_to_expl(rec, index);

	/*尝试获得thr对应执行事务对rec行锁的LOCK_X执行权*/
	err = lock_rec_lock(TRUE, LOCK_X, rec, index, thr);

	lock_mutex_exit_kernel();

	ut_ad(lock_rec_queue_validate(rec, index));

	return err;
}

/*通过二级索引修改记录行，激活一个等待的LOCK_X锁，如果成功，设置PAGE的操作trx_id*/
ulint lock_sec_rec_modify_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, que_thr_t* thr)
{
	if(flags & BTR_NO_LOCKING_FLAG)
		return DB_SUCCESS;

	ut_ad(!(index->type & DICT_CLUSTERED));

	lock_mutex_enter_kernel();
	
	ut_ad(lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
	/*在rec行上激活一个lock_x事务执行权*/
	err = lock_rec_lock(TRUE, LOCK_X, rec, index, thr);

	lock_mutex_exit_kernel();

	ut_ad(lock_rec_queue_validate(rec, index));

	if(err == DB_SUCCESS)
		page_update_max_trx_id(buf_frame_algin(rec), thr_get_trx(thr)->id)
}

/*通过二级索引读取记录行，如果记录的二级索引存在一个隐式锁，转换成显示锁（LOCK_X）,并尝试获得事务的锁执行权*/
ulint lock_sec_rec_read_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, ulint mode, que_thr_t* thr)
{
	ulint err;

	ut_ad(!(index->type & DICT_CLUSTERED));
	ut_ad(page_rec_is_user_rec(rec) || page_rec_is_supremum(rec));

	if(flags & BTR_NO_LOCKING_FLAG)
		return DB_SUCCESS;

	lock_mutex_enter_kernel();

	ut_ad(mode != LOCK_X || lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
	ut_ad(mode != LOCK_S || lock_table_has(thr_get_trx(thr), index->table, LOCK_IS));

	if((ut_dulint_cmp(page_get_max_trx_id(buf_frame_align(rec)), trx_list_get_min_trx_id()) >= 0|| recv_recovery_is_on()) 
		&& !page_rec_is_supremum(rec))
		lock_rec_convert_impl_to_expl(rec, index); /*在rec记录上加上一个LOCK_X行锁*/

	err = lock_rec_lock(FALSE, mode, rec, index, thr);

	lock_mutex_exit_kernel();

	ut_ad(lock_rec_queue_validate(rec, index));

	return err;
}

ulint lock_clust_rec_read_check_and_lock(ulint flags, rec_t* rec, dict_index_t* index, ulint mode, que_thr_t* thr)
{
	ulint	err;

	ut_ad(index->type & DICT_CLUSTERED);
	ut_ad(page_rec_is_user_rec(rec) || page_rec_is_supremum(rec));

	if(flags & BTR_NO_LOCKING_FLAG)
		return DB_SUCCESS;

	lock_mutex_enter_kernel();

	ut_ad(mode != LOCK_X || lock_table_has(thr_get_trx(thr), index->table, LOCK_IX));
	ut_ad(mode != LOCK_S || lock_table_has(thr_get_trx(thr), index->table, LOCK_IS));

	if(!page_rec_is_supremum(rec))
		lock_rec_convert_impl_to_expl(rec, index); /*在记录行上加上一个LOCK_X锁*/

	err = lock_rec_lock(FALSE, mode, rec, index, thr);

	lock_mutex_exit_kernel();

	ut_ad(lock_rec_queue_validate(rec, index));

	return err;
}

/************************************************************************/



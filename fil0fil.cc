#include "fil0fil.h"

#include "mem0mem.h"
#include "sync0sync.h"
#include "hash0hash.h"
#include "os0file.h"
#include "os0sync.h"
#include "mach0data.h"
#include "ibuf0ibuf.h"
#include "buf0buf.h"
#include "log0log.h"
#include "log0recv.h"
#include "fsp0fsp.h"
#include "srv0srv.h"


ulint fil_n_pending_log_flushes = 0;
ulint fil_n_pending_tablespace_flushes = 0;

fil_addr_t fil_addr_null = {FIL_NULL, 0};

typedef struct fil_node_struct fil_node_t;

#define	FIL_NODE_MAGIC_N	89389
#define	FIL_SPACE_MAGIC_N	89472

#define FIL_SYSTEM_HASH_SIZE 500

struct fil_node_struct
{
	char*		name;				/*文件路径名*/
	ibool		open;				/*文件是否被打开*/
	os_file_t	handle;				/*文件句柄*/
	ulint		size;				/*文件包含的页个数，一个页是16K*/
	ulint		n_pending;			/*等待读写IO操作的个数*/
	ibool		is_modified;		/*是否有脏也存在，也就是内存cache和硬盘数据不一致*/
	ulint		magic_n;			/*魔法校验字*/
	UT_LIST_NODE_T(fil_node_t) chain;
	UT_LIST_NODE_T(fil_node_t) LRU;
};

struct fil_space_struct
{
	char*			name;				/*space名称*/
	ulint			id;					/*space id*/
	ulint			purpose;			/*space的类型，主要有space table, log file和arch file*/
	ulint			size;				/*space包含的页个数*/
	ulint			n_reserved_extents; /*占用的页个数*/
	hash_node_t		hash;				/*chain node的HASH表*/
	rw_lock_t		latch;				/*space操作保护锁*/
	ibuf_data_t*	ibuf_data;			/*space 对应的insert buffer*/
	ulint			magic_n;			/*魔法校验字*/

	UT_LIST_BASE_NODE_T(fil_node_t) chain;
	UT_LIST_NODE_T(fil_space_t)		space_list;
};

typedef struct fil_system_struct
{
	mutex_t			mutex;				/*file system的保护锁*/
	hash_table_t*	spaces;				/*space的哈希表，用于快速检索space,一般是通过space id查找*/
	ulint			n_open_pending;		/*当前有读写IO操作的fil_node个数*/
	ulint			max_n_open;			/*最大允许打开的文件个数*/
	os_event_t		can_open;			/*可以打开新的文件的信号*/
	
	UT_LIST_BASE_NODE_T(fil_node_t) LRU;			/*最近被打开操作过的文件,用于快速定位关闭的fil_node*/
	UT_LIST_BASE_NODE_T(fil_node_t) space_list;		/*file space的对象列表*/
}fil_system_t;


fil_system_t* fil_system = NULL;


void fil_reserve_right_to_open()
{
loop:
	mutex_enter(&(fil_system->mutex));

	/*文件打开数量已经到了最大限制*/
	if(fil_system->n_open_pending == fil_system->max_n_open){ /*等待有文件关闭*/
		os_event_reset(fil_system->can_open);
		mutex_exit(&(fil_system->mutex));
		os_event_wait(fil_system->can_open);

		goto loop;
	}

	fil_system->max_n_open --;
	mutex_exit(&(fil_system->mutex));
}

void fil_release_right_to_open()
{
	mutex_enter(&(fil_system->mutex));
	/*有可以打开的文件名额*/
	if(fil_system->n_open_pending == fil_system->max_n_open)
		os_event_set(fil_system->can_open); /*发送可以打开文件的信号*/

	fil_system->max_n_open ++;
	mutex_exit(&(fil_system->mutex));
}

rw_lock_t* fil_space_get_latch(ulint id)
{
	fil_space_t* space;
	fil_system_t* sys = fil_system;
	ut_ad(system);

	mutex_enter(&(sys->mutex));
	/*找到对应的sapce*/
	HASH_SEARCH(hash, sys->spaces, id, space, space->id == id);
	mutex_exit(&(system->mutex));
	
	return &(space->latch);
}

ulint fil_space_get_type(ulint id)
{
	fil_space_t*	space;
	fil_system_t*	system	= fil_system;
	ut_ad(system);

	mutex_enter(&(system->mutex));
	HASH_SEARCH(hash, system->spaces, id, space, space->id == id);
	mutex_exit(&(system->mutex));

	return (space->purpose);
}

ibuf_data_t* fil_space_get_ibuf_data(ulint id)
{
	fil_space_t* space;
	fil_system_t* sys = fil_system;
	ut_ad(system);

	mutex_enter(&(system->mutex));
	HASH_SEARCH(hash, system->spaces, id, space, space->id == id);
	mutex_exit(&(system->mutex));

	return (space->ibuf_data);
}

void fil_node_create(char* name, ulint size, ulint id)
{
	fil_node_t* node;
	fil_space_t* space;
	char* name2;
	fil_system_t* sys = fil_system;

	ut_a(system);
	ut_a(name);
	ut_a(size > 0);

	mutex_enter(&(sys->mutex));
	node = mem_alloc(sizeof(fil_node_t));
	name2 = mem_alloc(ut_strlen(name) + 1);
	ut_strcpy(name2, name);

	node->name = name2;
	node->open = FALSE;
	node->size = size;
	node->magic_n = FIL_NODE_MAGIC_N;
	node->n_pending = 0;

	node->is_modified = FALSE;

	/*找到对应的space*/
	HASH_SEARCH(hash, sys->spaces, id, space, space->id == id);
	space->size += size;

	UT_LIST_ADD_LAST(chain, space->chain, node);
	mutex_exit(&(sys->mutex));
}

/*关闭一个文件*/
static void fil_node_close(fil_node_t* node, fil_system_t* system)
{
	ibool ret;

	ut_ad(node && system);
	ut_ad(mutex_own(&(system->mutex)));
	ut_a(node->open);
	ut_a(node->n_pending == 0);

	ret = os_file_close(node->handle);
	ut_a(ret);
	node->open = FALSE;

	/*从系统的LRU列表列表中删除*/
	UT_LIST_REMOVE(LRU, system->LRU, node);
}

static void fil_node_free(fil_node_t* node, fil_system_t* system, fil_space_t* space)
{
	ut_ad(node && system && space);
	ut_ad(mutex_own(&(system->mutex)));
	ut_a(node->magic_n == FIL_NODE_MAGIC_N);

	if(node->open)
		fil_node_close(node, system);

	space->size -= node->size;
	UT_LIST_REMOVE(chain, space->chain, node);
	
	mem_free(node->name);
	mem_free(node);
}

/*从space中删除fil_node，删除的总数据长度为trunc_len*/
void fil_space_truncate_start(ulint id, ulint trunc_len)
{
	fil_node_t*	node;
	fil_space_t*	space;
	fil_system_t*	system	= fil_system;

	mutex_enter(&(system->mutex));

	HASH_SEARCH(hash, system->spaces, id, space, space->id == id);
	ut_a(space);

	/*从头开始删除，知道删除的长度到trunc_len*/
	while(trunc_len > 0){
		node =  UT_LIST_GET_FIRST(space->chain);
		ut_a(node->size * UNIV_PAGE_SIZE >= trunc_len);
		trunc_len -= node->size * UNIV_PAGE_SIZE;

		fil_node_free(node, system, space);
	}

	mutex_exit(&(system->mutex));
}

/*创建一个fil_system*/
static fil_system_t* fil_system_create(ulint hash_size, ulint max_n_open)
{
	fil_system_t*	system;
	ut_a(hash_size > 0);
	ut_a(max_n_open > 0);

	system = mem_alloc(sizeof(fil_system_t));

	mutex_create(&(system->mutex));
	mutex_set_level(&(system->mutex), SYNC_ANY_LATCH);

	/*建立space hash table*/
	system->spaces = hash_create(hash_size);
	UT_LIST_INIT(system->LRU);

	system->n_open_pending = 0;
	system->max_n_open = max_n_open;
	system->can_open = os_event_create(NULL);

	UT_LIST_INIT(system->spaces);

	return system;
}

/*初始化fil模块，建立一个全局的fil_system*/
void fil_init(ulint max_n_open)
{
	ut_ad(fil_system);
	fil_system = fil_system_create(FIL_SYSTEM_HASH_SIZE, max_n_open);
}

void fil_ibuf_init_at_db_start()
{
	fil_space_t* space = UT_LIST_GET_FIRST(fil_system->space_list);
	while(space){
		if(space->purpose == FIL_TABLESPACE) /*是表文件,进行ibuf的初始化*/
			space->ibuf_data = ibuf_data_init_for_space(space->id);

		space = UT_LIST_GET_NEXT(space_list, space);
	}
}

static ulint fil_write_lsn_and_arch_no_to_file(ulint space_id, ulint sum_of_sizes, dulint lsn, ulint arch_log_no)
{
	byte*	buf1;
	byte*	buf;

	buf1 = mem_alloc(2 * UNIV_PAGE_SIZE);
	buf = ut_align(buf1, UNIV_PAGE_SIZE);
	/*从sum_of_size的位置读取一个16k的数据页*/
	fil_read(TRUE, space_id, sum_of_sizes, 0, UNIV_PAGE_SIZE, buf, NULL);

	/*将lsn和arch_log_no写入到buf和文件*/
	mach_write_to_8(buf + FIL_PAGE_FILE_FLUSH_LSN, lsn);
	mach_write_to_4(buf + FIL_PAGE_ARCH_LOG_NO, arch_log_no);
	fil_write(TRUE, space_id, sum_of_sizes, 0, UNIV_PAGE_SIZE, buf, NULL);

	return DB_SUCCESS;
}

ulint fil_write_flushed_lsn_to_data_files(dulint lsn, ulint arch_log_no)
{
	fil_space_t*	space;
	fil_node_t*	node;
	ulint		sum_of_sizes;
	ulint		err;

	mutex_enter(&(fil_system->mutex));

	space = UT_LIST_GET_FIRST(fil_system->space_list);
	while(space != NULL){
		if(space->purpose == FIL_TABLESPACE){ /*表空间文件*/
			node = UT_LIST_GET_FIRST(space->chain);

			while(node != NULL){
				mutex_exit(&(fil_system->mutex));

				/*写入lsn和arch_log_no到page中*/
				err = fil_write_lsn_and_arch_no_to_file(space->id, sum_of_sizes, lsn, arch_log_no);
				if(err != DB_SUCCESS)
					return err;

				mutex_enter(&(fil_system->mutex));

				sum_of_sizes += node->size;
				node = UT_LIST_GET_NEXT(chain, node);
			}
		}

		space = UT_LIST_GET_NEXT(space_list, space);
	}
}

void fil_read_flushed_lsn_and_arch_log_no(os_file_t data_file, ibool one_read_already, 
	dulint* min_flushed_lsn, ulint* min_arch_log_no, dulint* max_flushed_lsn, ulint* max_arch_log_no)
{
	byte*	buf;
	byte*	buf2;
	dulint	flushed_lsn;
	ulint	arch_log_no;

	buf2 = ut_malloc(2 * UNIV_PAGE_SIZE);
	buf = ut_align(buf2, UNIV_PAGE_SIZE);

	/*从文件中读取一个page的数据*/
	os_file_read(data_file, buf, 0, 0, UNIV_PAGE_SIZE);
	/*从page信息中获得flush lsn 和arch log no*/
	flushed_lsn = mach_read_from_8(buf + FIL_PAGE_FILE_FLUSH_LSN);
	arch_log_no = mach_read_from_4(buf + FIL_PAGE_ARCH_LOG_NO);

	ut_free(buf2);

	if (!one_read_already){
		*min_flushed_lsn = flushed_lsn;
		*max_flushed_lsn = flushed_lsn;
		*min_arch_log_no = arch_log_no;
		*max_arch_log_no = arch_log_no;

		return;
	}

	/*有值传入，需要进行对比*/
	if (ut_dulint_cmp(*min_flushed_lsn, flushed_lsn) > 0)
		*min_flushed_lsn = flushed_lsn;

	if (ut_dulint_cmp(*max_flushed_lsn, flushed_lsn) < 0)
		*max_flushed_lsn = flushed_lsn;

	if (*min_arch_log_no > arch_log_no)
		*min_arch_log_no = arch_log_no;

	if (*max_arch_log_no < arch_log_no)
		*max_arch_log_no = arch_log_no;
}

void fil_space_create(char* name, ulint id, ulint purpose)
{
	fil_space_t* space;
	char* name2;
	fil_system_t* system = fil_system;

	ut_a(system);
	ut_a(name);

#ifndef UNIV_BASIC_LOG_DEBUG
	ut_a((purpose == FIL_LOG) || (id % 2 == 0));
#endif

	mutex_enter(&(system->mutex));
	space = mem_alloc(sizeof(fil_space_t));
	name2 = mem_alloc(ut_strlen(name) + 1);

	ut_strcpy(name2, name);

	space->name = name2;
	space->id = id;
	space->purpose = purpose;
	space->size = 0;
	space->n_reserved_extents = 0;

	UT_LIST_INIT(space->chain);
	space->magic_n = FIL_SPACE_MAGIC_N;
	space->ibuf_data = NULL;
	/*创建latch*/
	rw_lock_create(&(space->latch));
	rw_lock_set_level(&(space->latch), SYNC_FSP);
	/*插入fil system当中*/
	HASH_INSERT(fil_space_t, hash, system->spaces, id, space);
	UT_LIST_ADD_LAST(space_list, system->space_list, space);

	mutex_exit(&(system->mutex));
}

void fil_space_free(ulint id)
{
	fil_space_t*	space;
	fil_node_t*	fil_node;
	fil_system_t*	system 	= fil_system;

	/*从fil_system的hash table中找到对应的space并从fil_system表中删除*/
	HASH_SEARCH(hash, system->spaces, id, space, space->id == id);
	HASH_DELETE(fil_space_t, hash, system->spaces, id, space);
	UT_LIST_REMOVE(space_list, system->space_list, space);

	/*魔法字校验*/
	ut_ad(space->magic_n == FIL_SPACE_MAGIC_N);

	/*释放space中的fil_node*/
	fil_node = UT_LIST_GET_FIRST(space->chain);
	ut_d(UT_LIST_VALIDATE(chain, fil_node_t, space->chain));
	while(fil_node != NULL){
		/*逐个释放fil_node*/
		fil_node_free(fil_node, system, space);
		fil_node = UT_LIST_GET_FIRST(space->chain);
	}

	ut_d(UT_LIST_VALIDATE(chain, fil_node_t, space->chain));
	ut_ad(0 == UT_LIST_GET_LEN(space->chain));

	mutex_exit(&(system->mutex));
	/*释放space的内存空间*/
	mem_free(space->name);
	mem_free(space);
}

ulint fil_space_get_size(ulint id)
{
	fil_space_t*	space;
	fil_system_t*	system = fil_system;
	ulint		    size = 0;
	
	ut_ad(system);
	mutex_enter(&(system->mutex));
	HASH_SEARCH(hash, system->spaces, id, space, space->id == id);
	size = space->size;
	mutex_exit(&(system->mutex));

	return size;
}

ibool fil_check_adress_in_tablespace(ulint id, ulint page_no)
{
	fil_space_t*	space;
	fil_system_t*	system = fil_system;
	ulint		    size = 0;
	ibool			ret;

	ut_ad(system);

	mutex_enter(&(system->mutex));
	HASH_SEARCH(hash, system->spaces, id, space, space->id == id);
	if(sapce == NULL)
		ret = FALSE;
	else{
		size = space->size;
		if(page_no > size) /*page no不属于这个space*/
			return FALSE;
		else if(space->purpose != FIL_TABLESPACE) /*这个space不是表空间类型*/
			return FALSE;
		else
			ret = TRUE;
	}

	mutex_exit(&(system->mutex));

	return ret;
}

/*预留指定的空闲空间*/
ibool fil_space_reserve_free_extents(ulint id, ulint n_free_now, ulint n_to_reserve)
{
	fil_space_t* space;
	fil_system_t* system = fil_system;
	ibool success;

	ut_ad(system);
	/*查找对应的space*/
	mutex_enter(&(system->mutex));
	HASH_SEARCH(hash, system->spaces, id, space, space->id == id);

	/*对预留的合法性判断，如果space的已经空闲的区域 + 指定需要预留的n_to_reserve之和 与n_free_now不匹配,表示预留失败*/
	if(space->n_reserved_extents + n_to_reserve > n_free_now)
		success = FALSE;
	else{
		space->n_reserved_extents += n_to_reserve;
		success = TRUE;
	}

	mutex_exit(&(system->mutex));

	return success;
}

/*缩小占用范围*/
void fil_space_release_free_extents(ulint id, ulint n_reserved)
{
	fil_space_t* space;
	fil_system_t* system = fil_system;

	mutex_enter(&(system->mutex));

	HASH_SEARCH(hash, system->spaces, id, space, space->id == id);
	ut_a(space->n_reserved_extents >= n_reserved);
	space->n_reserved_extents -= n_reserved;

	mutex_exit(&(system->mutex));
}

/*为fil_node做IO操作准备和校验，同时打开fil_node中的文件*/
static void fil_node_prepare_for_io(fil_node_t* node, fil_system_t* system, fil_space_t* space)
{
	ibool ret; 
	fil_node_t* last_node;
	
	/*fil_node对应的文件是关闭的*/
	if(!node->open){
		ut_a(node->n_pending == 0);

		/*判断是否可以打开新的文件,如果打开的文件数已经达到系统的上限，关闭其中一个*/
		if(system->n_open_pending + UT_LIST_GET_LEN(system->LRU) == system->max_n_open){
			ut_a(UT_LIST_GET_LEN(system->LRU) > 0);
			/*从置换队列中取出最后一个fil_node*/
			last_node = UT_LIST_GET_LAST(system->LRU);
			if (last_node == NULL) {
				fprintf(stderr, "InnoDB: Error: cannot close any file to open another for i/o\n"
					"InnoDB: Pending i/o's on %lu files exist\n", system->n_open_pending);

				ut_a(0);
			}
			/*n_open_pending的状态计数器会在这个函数统一调用*/
			fil_node_close(last_node, system);
		}

		if(space->purpose == FIL_LOG)
			node->handle = os_file_create(node->name, OS_FILE_OPEN, OS_FILE_AIO, OS_LOG_FILE, &ret);
		else
			node->handle = os_file_create(node->name, OS_FILE_OPEN, OS_DATA_FILE, OS_LOG_FILE, &ret);

		ut_a(ret);
		/*更改状态*/
		node->open = TRUE;
		system->n_open_pending ++;
		node->n_pending = 1;

		return;
	}

	/*假如文件是打开的,并且在LRU当中*/
	if(node->n_pending == 0){
		/*从LRU队列中删除*/
		UT_LIST_REMOVE(LRU, system->LRU, node);
		system->n_open_pending ++;
		node->n_pending = 1;
	}
	else /*只增加io操作的计数器*/
		node->n_pending ++;
}

/*io操作完成后，更新对应的状态,并把node放到LRU队列当中*/
static void fil_node_complete_io(fil_node_t* node, fil_system_t* system, ulint type)
{
	ut_ad(node);
	ut_ad(system);
	ut_ad(mutex_own(&(system->mutex)));
	ut_a(node->n_pending > 0);

	node->n_pending --;
	/*不是读操作，将说明cache被改动过,磁盘和cache不一致*/
	if(type != OS_FILE_READ)
		node->is_modified = TRUE;

	/*没有其他的io操作正在进行*/
	if(node->n_pending == 0){
		UT_LIST_ADD_FIRST(LRU, system->LRU, node);

		ut_a(system->n_open_pending > 0);
		system->n_open_pending --;
		/*可以打开更多的文件，发送对应信号*/
		if(system->n_open_pending == system->max_n_open - 1)
			os_event_set(system->can_open);
	}
}

ibool fil_extend_last_data_file(ulint* actual_increase, ulint size_increase)
{
	fil_node_t*		node;
	fil_space_t*	space;
	fil_system_t*	system	= fil_system;

	byte*		buf;
	ibool		success;
	ulint		i;

	mutex_enter(&(system->mutex));
	HASH_SEARCH(hash, system->spaces, 0, space, space->id == 0);
	node = UT_LIST_GET_LAST(space->chain);

	/*进行io操作判断并打开文件*/
	fil_node_prepare_for_io(node, system, space);
	buf = mem_alloc(1024 * 1024);
	memset(buf, 0, 1024 * 1024);
	
	/*以1M为单位写操作*/
	for(i = 0; i < size_increase / ((1024 * 1024) / UNIV_PAGE_SIZE); i ++){
		success = os_file_write(node->name, node->handle, buf, (node->size << UNIV_PAGE_SIZE_SHIFT) & 0xFFFFFFFF,
			node->size >> (32 - UNIV_PAGE_SIZE_SHIFT),1024 * 1024);
		if(!success)
			break;

		node->size += (1024 * 1024) / UNIV_PAGE_SIZE;
		space->size += (1024 * 1024) / UNIV_PAGE_SIZE;
		/*磁盘有空闲空间*/
		os_has_said_disk_full = FALSE;
	}

	mem_free(buf);
	/*对IO完成，更改对应node的状态信息*/
	fil_node_complete_io(node, system, OS_FILE_WRITE);
	mutex_exit(&(system->mutex));

	/*实际写入的page数*/
	*actual_increase = i * ((1024 * 1024) / UNIV_PAGE_SIZE);
	/*node数据刷盘*/
	fil_flush(0);

	srv_data_file_sizes[srv_n_data_files - 1] += *actual_increase;

	return TRUE;
}

void fil_io(ulint type, ibool sync, ulint space_id, ulint block_offset, ulint byte_offset, ulint len, void* buf, void* message)
{
	ulint			mode;
	fil_space_t*	space;
	fil_node_t*		node;
	ulint			offset_high;
	ulint			offset_low;
	fil_system_t*	system;
	os_event_t		event;
	ibool			ret;
	ulint			is_log;
	ulint			wake_later;
	ulint			count;

	is_log = type & OS_FILE_LOG;
	type = type & ~OS_FILE_LOG;

	wake_later = type & OS_AIO_SIMULATED_WAKE_LATER;
	type = type & ~OS_AIO_SIMULATED_WAKE_LATER;

	ut_ad(byte_offset < UNIV_PAGE_SIZE);
	ut_ad(buf);
	ut_ad(len > 0);
	ut_ad((1 << UNIV_PAGE_SIZE_SHIFT) == UNIV_PAGE_SIZE);
	ut_ad(fil_validate());

#ifndef UNIV_LOG_DEBUG
	/* ibuf bitmap pages must be read in the sync aio mode: ibuf bitmap的page必须是同步读取的*/
	ut_ad(recv_no_ibuf_operations || (type == OS_FILE_WRITE) || !ibuf_bitmap_page(block_offset) || sync || is_log);
#ifdef UNIV_SYNC_DEBUG
	ut_ad(!ibuf_inside() || is_log || (type == OS_FILE_WRITE)
		|| ibuf_page(space_id, block_offset));
#endif
#endif

	/*文件打开模式的判断*/
	if(sync)
		mode = OS_AIO_SYNC;
	else if(type == OS_FILE_READ && !is_log && ibuf_page(space_id, block_offset))
		mode = OS_AIO_IBUF;
	else if(is_log)
		mode = OS_AIO_LOG;
	else 
		mode = OS_AIO_NORMAL;

	system = fil_system;
	count = 0;

loop:
	count ++;
	mutex_enter(&(system->mutex));
	/*当前的读写io操作超过了最大限度的3/4,防止挂起*/
	if(count < 500 && !is_log && ibuf_inside() && system->n_open_pending >= (3 * system->max_n_open) / 4){
		mutex_exit(&(system->mutex));
		/*唤醒aio的操作线程进行操作*/
		os_aio_simulated_wake_handler_threads();
		os_thread_sleep(100000);

		if(count > 50)
			fprintf(stderr, "InnoDB: Warning: waiting for file closes to proceed\n" 
				"InnoDB: round %lu\n", count);
		/*继续判断*/
		goto loop;
	}

	/*当前的读写IO操作数已经到了上限*/
	if(system->n_open_pending == system->max_n_open){
		event = system->can_open;
		os_event_reset(event);
		mutex_exit(&(system->mutex));

		/*唤醒aio的操作线程进行操作*/
		os_aio_simulated_wake_handler_threads();
		/*等待允许打开文件的信号*/
		os_event_wait(event);

		goto loop;
	}
	/*查找需要io操作的space*/
	HASH_SEARCH(hash, system->spaces, space_id, space, space->id == space_id);
	ut_a(space);
	ut_ad((mode != OS_AIO_IBUF) || (space->purpose == FIL_TABLESPACE));

	node = UT_LIST_GET_FIRST(space->chain);
	for(;;){
		if (node == NULL) {
			fprintf(stderr,
				"InnoDB: Error: trying to access page number %lu in space %lu\n"
				"InnoDB: which is outside the tablespace bounds.\n"
				"InnoDB: Byte offset %lu, len %lu, i/o type %lu\n", block_offset, space_id, byte_offset, len, type);
			ut_a(0);
		}
		/*定位到需要操作的node位置,根据page对应关系找到对应的位置*/
		if(node->size > block_offset)
			break;
		else{
			block_offset -= node->size;
			node = UT_LIST_GET_NEXT(chain, node);
		}
	}
	/*进行io操作判断并打开文件*/
	fil_node_prepare_for_io(node, system, space);
	mutex_enter(&(system->mutex));

	/*计算高位偏移和低位偏移*/
	offset_high = (block_offset >> (32 - UNIV_PAGE_SIZE_SHIFT));
	offset_low  = ((block_offset << UNIV_PAGE_SIZE_SHIFT) & 0xFFFFFFFF) + byte_offset;

	ut_a(node->size - block_offset >= (byte_offset + len + (UNIV_PAGE_SIZE - 1)) / UNIV_PAGE_SIZE);
	ut_a(byte_offset % OS_FILE_LOG_BLOCK_SIZE == 0);
	ut_a((len % OS_FILE_LOG_BLOCK_SIZE) == 0);
	/*进行aio调用*/
	ret = os_aio(type, mode | wake_later, node->name, node->handle, buf,
		offset_low, offset_high, len, node, message);
	ut_a(ret);

	if(mode == OS_AIO_SYNC){ /*同步调用，会在os_aio中刷盘*/
		mutex_enter(&(system->mutex));
		/*io完成，更新对应的node状态*/
		fil_node_complete_io(node, system, type);
		mutex_exit(&(system->mutex));

		ut_ad(fil_validate());
	}
}

void fil_read(ibool sync, ulint space_id, ulint block_offset, ulint byte_offset, ulint len, void* buf, void* message)
{
	fil_io(OS_FILE_READ, sync, space_id, block_offset, byte_offset, len, buf, message);
}

void fil_write(ibool sync, ulint space_id, ulint block_offset, ulint byte_offset, ulint len, void* buf, void* message)
{
	fil_io(OS_FILE_WRITE, sync, space_id, block_offset, byte_offset, len, buf, message);
}

void fil_aio_wait(ulint segment)
{		
	fil_node_t*		fil_node;
	fil_system_t*	system	= fil_system;
	void*			message;
	ulint			type;
	ibool			ret;

	ut_ad(fil_validate());

	if(os_aio_use_native_aio){ /*用系统的aio*/
		srv_io_thread_op_info[segment] = "native aio handle";
#ifdef POSIX_ASYNC_IO
		ret = os_aio_posix_handle(segment, &fil_node, &message);
#else
		ret = 0; /* Eliminate compiler warning */
		ut_a(0);
#endif
	}
	else{ /*模拟的aio*/
		srv_io_thread_op_info[segment] = "simulated aio handle";
		ret = os_aio_simulated_handle(segment, (void**)&fil_node, &message, &type);
	}

	ut_a(ret);
	srv_io_thread_op_info[segment] = "complete io for fil node";
	
	mutex_enter(&(system->mutex));
	/*异步完成了IO,更改对应的fil_node状态*/
	fil_node_complete_io(fil_node, fil_system, type);
	mutex_exit(&(system->mutex));

	if(buf_pool_is_block(message)){ /*page刷盘完成*/
		srv_io_thread_op_info[segment] = "complete io for buf page";
		buf_page_io_complete(message);
	}
	else{ /*日志刷盘完成*/
		srv_io_thread_op_info[segment] = "complete io for log";
		log_io_complete(message);
	}
}

/*对space刷盘*/
void fil_flush(ulint space_id)
{
	fil_system_t*	system	= fil_system;
	fil_space_t*	space;
	fil_node_t*		node;
	os_file_t		file;

	mutex_enter(&(system->mutex));
	HASH_SEARCH(hash, system->spaces, space_id, space, space->id == space_id);
	ut_a(space);

	node = UT_LIST_GET_FIRST(space->chain);
	while(node){
		if(node->open && node->is_modified){ /*存在脏页，需要进行flush*/
			node->is_modified = FALSE;
			file = node->handle;
			
			if(space->purpose == FIL_TABLESPACE)
				fil_n_pending_tablespace_flushes ++;
			else
				fil_n_pending_log_flushes ++;

			mutex_exit(&(system->mutex));

			/*进行flush*/
			os_file_flush(file);

			mutex_enter(&(system->mutex));
			if(space->purpose == FIL_TABLESPACE)
				fil_n_pending_tablespace_flushes --;
			else
				fil_n_pending_log_flushes --;
		}

		node = UT_LIST_GET_NEXT(chain, node);
	}

	mutex_exit(&(system->mutex));
}

/*对purpose的space进行刷盘*/
void fil_flush_file_spaces(ulint purpose)
{
	fil_system_t*	system	= fil_system;
	fil_space_t*	space;

	mutex_enter(&(system->mutex));

	space = UT_LIST_GET_FIRST(system->space_list);
	while(space){
		if (space->purpose == purpose) {
			mutex_exit(&(system->mutex));
			fil_flush(space->id);

			mutex_enter(&(system->mutex));
		}

		space = UT_LIST_GET_NEXT(space_list, space);
	}
	mutex_exit(&(system->mutex));
}

ibool fil_validate(void)

{	
	fil_space_t*	space;
	fil_node_t*	fil_node;
	ulint		pending_count	= 0;
	fil_system_t*	system;
	ulint		i;

	system = fil_system;

	mutex_enter(&(system->mutex));

	for (i = 0; i < hash_get_n_cells(system->spaces); i++) {
		space = HASH_GET_FIRST(system->spaces, i);
		while (space != NULL) {
			UT_LIST_VALIDATE(chain, fil_node_t, space->chain); 
			
			fil_node = UT_LIST_GET_FIRST(space->chain);
			while (fil_node != NULL) {
				if (fil_node->n_pending > 0) {
					pending_count++;
					ut_a(fil_node->open);
				}

				fil_node = UT_LIST_GET_NEXT(chain, fil_node);
			}

			space = HASH_GET_NEXT(hash, space);
		}
	}

	ut_a(pending_count == system->n_open_pending);
	UT_LIST_VALIDATE(LRU, fil_node_t, system->LRU);
	fil_node = UT_LIST_GET_FIRST(system->LRU);

	while (fil_node != NULL) {
		ut_a(fil_node->n_pending == 0);
		ut_a(fil_node->open);

		fil_node = UT_LIST_GET_NEXT(LRU, fil_node);
	}
	mutex_exit(&(system->mutex));

	return(TRUE);
}

ibool fil_addr_is_null(fil_addr_t addr)
{
	if(addr.page == FIL_NULL)
		return TRUE;
	else
		return FALSE;
}

ulint fil_page_get_prev(byte* page)
{
	return(mach_read_from_4(page + FIL_PAGE_PREV));
}

ulint fil_page_get_next(byte* page)
{
	return(mach_read_from_4(page + FIL_PAGE_NEXT));
}

void fil_page_set_type(byte* page, ulint type)
{
	ut_ad(page);
	ut_ad((type == FIL_PAGE_INDEX) || (type == FIL_PAGE_UNDO_LOG));

	mach_write_to_2(page + FIL_PAGE_TYPE, type);
}

ulint fil_page_get_type(byte* page)
{
	ut_ad(page);
	return mach_read_from_2(page + FIL_PAGE_TYPE);
}


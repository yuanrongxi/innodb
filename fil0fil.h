#ifndef __fil0fil_h_
#define __fil0fil_h_

#include "univ.h"
#include "sync0rw.h"
#include "dict0types.h"
#include "ibuf0types.h"
#include "ut0byte.h"
#include "os0file.h"

/*-1*/
#define FIL_NULL	ULINT32_UNDEFINED

/*space address type*/
typedef byte fil_faddr_t;

#define FIL_ADDR_PAGE	0
#define FIL_ADDR_BYTE	4
#define FIL_ADDR_SIZE	6
#endif

typedef struct fil_addr_struct
{
	ulint	page;		/*page在space中的编号*/
	ulint	boffset;	/*page在space中的偏移量*/
}fil_addr_t;

extern fil_addr_t fil_addr_null;

/*文件页中变量对应的偏移量*/
#define FIL_PAGE_SPACE				0
#define FIL_PAGE_OFFSET				4
#define FIL_PAGE_PREV				8
#define FIL_PAGE_NEXT				12
#define FIL_PAGE_LSN				16
#define FIL_PAGE_TYPE				24
#define FIL_PAGE_FILE_FLUSH_LSN		26
#define FIL_PAGE_ARCH_LOG_NO		34
#define FIL_PAGE_DATA				38

/*文件页的尾信息*/
#define FIL_PAGE_END_LSN			8
#define FIL_PAGE_DATA_END			8

/*文件页类型*/
#define FIL_PAGE_INDEX				17855
#define FIL_PAGE_UNDO_LOG			2

/* Space types */
#define FIL_TABLESPACE 				501
#define FIL_LOG						502

/*重做日志文件正在刷盘操作的计数*/
extern ulint fil_n_pending_log_flushes;
/*表文件正在刷盘的计数*/
extern ulint fil_n_pending_tablespace_flushes;

/**********************函数********************/
void		fil_reserve_right_to_open();

void		fil_release_right_to_open();
	
ibool		fil_addr_is_null(fil_addr_t addr);

void		fil_init(ulint max_open);

void		fil_ibuf_init_at_db_start();

void		fil_space_create(char* name, ulint id, ulint purpose);

void		fil_space_truncate_start(ulint id, ulint trunc_len);
/*在最后一个fil_node上清除数据(数据用0填充),清除长度为size_increase*/
ibool		fil_extend_last_data_file(ulint* actual_increase, ulint size_increase);

void		fil_space_free(ulint id);

rw_lock_t*	fil_space_get_latch(ulint id);

ulint		fil_space_get_type(ulint id);

ulint		fil_write_flushed_lsn_to_data_files(dulint lsn, ulint arch_log_no);

void		fil_read_flushed_lsn_and_arch_log_no(os_file_t data_file, ibool one_read_already, dulint*	min_flushed_lsn, 
										  ulint* min_arch_log_no, dulint* max_flushed_lsn, ulint* max_arch_log_no);

ibuf_data_t* fil_space_get_ibuf_data(ulint id);

ulint		fil_space_get_size(ulint id);

ibool		fil_check_adress_in_tablespace(ulint id, ulint page_no);

void		fil_node_create(char* name, ulint size, ulint id);
/*io操作函数，主要用于读写调用*/
void		fil_io(ulint type, ibool sync, ulint space_id, ulint block_offset, ulint byte_offset, ulint len, void* buf, void* message);
/*io read操作*/
void		fil_read(ibool sync, ulint space_id, ulint block_offset, ulint byte_offset, ulint len, void* buf, void* message);
/*io write操作*/
void		fil_write(ibool sync, ulint space_id, ulint block_offset, ulint byte_offset, ulint len, void* buf, void* message);

void		fil_aio_wait(ulint segment);

void		fil_flush(ulint space_id);

void		fil_flush_spaces(ulint purpose);

ibool		fil_validate();

/*********文件页的访问函数**************/
ulint		fil_page_get_prev(byte* page);

ulint		fil_page_get_next(byte* page);

void		fil_page_set_type(byte* page);

ulint		fil_page_get_type(byte* page);

ibool		fil_space_reserve_free_extents(ulint id, ulint n_free_now, ulint n_to_reserve); 

void		fil_space_release_free_extents(ulint id, ulint n_reserved);


typedef struct fil_space_struct fil_space_t;
#include "os0file.h"
#include "mach0data.h"
#include "mtr0mtr.h"

ibool log_ceck_log_rec(byte* buf, ulint len, dulint buf_start_lsn);

/*检查block的flush bit*/
UNIV_INLINE ibool log_block_get_flush_bit(byte* log_block)
{
	if(LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO))
		return TRUE;
	return FALSE;
}

/*设置block的flush bit*/
UNIV_INLINE void log_block_set_flush_bit(byte* log_block, ibool val)
{
	ulint field = mach_read_from_4(log_block + LOG_BLOCK_HDR_NO);

	if(val)
		field = field | LOG_BLOCK_FLUSH_BIT_MASK;
	else
		field = field &(~LOG_BLOCK_FLUSH_BIT_MASK);

	mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, field);
}

/*获得log block number，block的第一个4字节除最高位以外的31位表示*/
UNIV_INLINE ulint log_block_get_hdr_no(byte* log_block)
{
	return (~LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(log_block + LOG_BLOCK_HDR_NO));
}

UNIV_INLINE void log_block_set_hdr_no(byte* log_block, ulint n)
{
	ut_ad(n > 0);
	ut_ad(n < LOG_BLOCK_FLUSH_BIT_MASK);

	mach_write_to_4(log_block + LOG_BLOCK_HDR_NO, n);
}

UNIV_INLINE ulint log_block_get_data_len(byte* log_block)
{
	return mach_read_from_2(log_block + LOG_BLOCK_HDR_DATA_LEN);
}

UNIV_INLINE void log_block_set_data_len(byte* log_block, ulint len)
{
	mach_write_to_2(log_block + LOG_BLOCK_HDR_DATA_LEN, len);
}

UNIV_INLINE ulint log_block_get_first_rec_group(byte* log_block)
{
	return mach_read_from_2(log_block + LOG_BLOCK_FIRST_REC_GROUP);
}

UNIV_INLINE void log_block_set_first_rec_group(byte* log_block, ulint offset)
{
	mach_write_to_2(log_block + LOG_BLOCK_FIRST_REC_GROUP, offset);
}

UNIV_INLINE ulint log_block_get_checkpoint_no(byte* log_block)
{
	return mach_read_from_4(log_block + LOG_BLOCK_CHECKPOINT_NO);
}

UNIV_INLINE void log_block_set_checkpoint_no(byte* log_block, dulint no)
{
	mach_write_to_4(log_block + LOG_BLOCK_CHECKPOINT_NO, no.low);
}

/*获得lsn对应的block number*/
UNIV_INLINE ulint log_block_convert_lsn_to_no(dulint lsn)
{
	ulint no; /* 0 < no and no < 1G*/
	/*获得number, no = lsn / OS_FILE_LOG_BLOCK_SIZE，lsn是多少个OS_FILE_LOG_BLOCK_SIZE*/
	no = lsn.low / OS_FILE_LOG_BLOCK_SIZE;
	no += (lsn.high % OS_FILE_LOG_BLOCK_SIZE) * 2  * (0x80000000 / OS_FILE_LOG_BLOCK_SIZE);
	no = no & 0x3FFFFFFF;

	return no + 1;
}

UNIV_INLINE ulint log_block_calc_checksum(byte* block)
{
	ulint sum;
	ulint sh;
	ulint i;

	sum = 1;
	sh = 0;
	/*最后四个字节应该是填写check sum*/
	for(i = 0; i < OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE, i ++){
		sum = sum & 0x7FFFFFFF;
		sum += (((ulint)(*(block + i))) << sh) + (ulint)(*(block + i));
		sh ++;
		if(sh > 24) /*大于24位的时候归零*/
			sh = 0;
	}

	return sum;
}

UNIV_INLINE ulint log_block_get_checksum(byte* log_block)
{
	return mach_read_from_4(log_block + OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_CHECKSUM);
}

UNIV_INLINE void log_block_set_checksum(byte* log_block, ulint checksum)
{
	mach_write_to_4(log_block + OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_CHECKSUM, checksum);
}

UNIV_INLINE VOID log_block_init(byte* log_block, dulint lsn)
{
	ulint no;
	ut_ad(mutex_own(&(log_sys->mutex)));

	/*通过lsn得到block number*/
	no = log_block_convert_lsn_to_no(lsn);
	log_block_set_hdr_no(log_block, no);

	log_block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
	/*设置first rec group*/
	log_block_set_first_rec_group(log_block, 0);
}

UNIV_INLINE void log_block_init_in_old_format(byte* log_block, dulint lsn)
{
	ulint	no;

	ut_ad(mutex_own(&(log_sys->mutex)));

	no = log_block_convert_lsn_to_no(lsn);

	log_block_set_hdr_no(log_block, no);
	mach_write_to_4(log_block + OS_FILE_LOG_BLOCK_SIZE- LOG_BLOCK_CHECKSUM, no); /*将no作为check sum??*/

	log_block_set_data_len(log_block, LOG_BLOCK_HDR_SIZE);
	log_block_set_first_rec_group(log_block, 0);
}

UNIV_INLINE dulint log_reserve_and_write_fast(byte* str, ulint len ,dulint start_len, ibool* success)
{
	log_t* log = log_sys;
	ulint data_len;
	dulint lsn;

	*success = TRUE;

	data_len = len + log->buf_free % OS_FILE_LOG_BLOCK_SIZE;
	/*在线备份或者buf_free与str无法凑成512字节块,快速写应该是一定能凑成512*/
	if(log->online_backup_state || data_len >= OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE){
		*success = FALSE;
		mutex_exit(&(log->mutex));
		return ut_dulint_zero;
	}

	*start_lsn = log->lsn;
	ut_memcpy(log->buf + log->buf_free, str, len);
	/*设置块的数据长度*/
	log_block_set_data_len(ut_align_down(log->buf + log->buf_free, OS_FILE_LOG_BLOCK_SIZE), data_len);

#ifdef UNIV_LOG_DEBUG
	log->old_buf_free = log->buf_free;
	log->old_lsn = log->lsn;
#endif

	log->buf_free += len;
	ut_ad(log->buf_free <= log->buf_size);
	/*生成新的lsn*/
	lsn = ut_dulint_add(log->lsn, len);
	log->lsn = lsn;

#ifdef UNIV_LOG_DEBUG
	log_check_log_recs(log->buf + log->old_buf_free, log->buf_free - log->old_buf_free, log->old_lsn);	
#endif

	return lsn;
}

UNIV_INLINE void log_release()
{
	mutex_exit(&(log_sys->mutex));
}

/*获得log_t的lsn*/
UNIV_INLINE dulint log_get_lsn()
{
	dulint	lsn;

	mutex_enter(&(log_sys->mutex));
	lsn = log_sys->lsn;
	mutex_exit(&(log_sys->mutex));

	return(lsn);
}

/*数据库在修改了4个以上的page是必须调用一次这个函数，这个线程没有在做锁同步，除了dictionary mutex以外*/
UNIV_INLINE void log_free_check()
{
	if(log_sys->check_flush_or_checkpoint) /*检查是否需要log buffer刷盘或者建立一个checkpoint*/
		log_check_margins();
}

UNIV_INLINE dulint log_get_online_backup_lsn_low()
{
	ut_ad(mutex_own(&(log_sys->mutex)));
	ut_ad(log_sys->online_backup_state);

	return(log_sys->online_backup_lsn);
}

UNIV_INLINE ibool log_get_online_backup_state_low(void)
{
	ut_ad(mutex_own(&(log_sys->mutex)));
	return(log_sys->online_backup_state);
}



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

/*获得lsn的block number*/
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

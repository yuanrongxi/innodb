#include "mach0data.h"

/*本文件是对dtype的封装*/

UNIV_INLINE void dtype_set(dtype_t* type, ulint mtype, ulint prtype, ulint len, ulint prec)
{
	ut_ad(type);
	ut_ad(mtype <= DATA_MTYPE_MAX);

	type->mtype = mtype;
	type->prec = prtype;
	type->len = len;
	type->prec = prec;

	ut_ad(dtype_validate(type));
}

UNIV_INLINE void dtype_copy(dtype_t* type1, dtype_t* type2)
{
	*type1 = *type2;
	ut_ad(dtype_validate(type1));
}

UNIV_INLINE ulint dtype_get_mtype(dtype_t* type)
{
	ut_ad(type);

	return type->mtype;
}

UNIV_INLINE ulint dtype_get_prtype(dtype_t* type)
{
	ut_ad(type);

	return type->prtype;
}

UNIV_INLINE ulint dtype_get_len(dtype_t* type)
{
	ut_ad(type);
	return type->len;
}

UNIV_INLINE ulint dtype_get_prec(dtype_t* type)
{
	ut_ad(type);
	return type->prec;
}

UNIV_INLINE ulint dtype_get_pad_char(dtype_t* type)
{
	if(type->mtype == DATA_CHAR || type->mtype == DATA_VARCHAR
		|| type->mtype == DATA_FIXBINARY || type->mtype == DATA_FIXBINARY){
			return ((ulint)' ');
	}

	return ULINT_UNDEFINED;
}

UNIV_INLINE void dtype_store_for_order_and_null_size(byte* buf, dtype_t* type)
{
	ut_ad(4 == DATA_ORDER_NULL_TYPE_BUF_SIZE);
	buf[0] = (byte)(type->mtype & 0xff);
	buf[1] = (byte)(type->prtype & 0xff);

	mach_write_to_2(buf + 2, type->len & 0xffff);
}

UNIV_INLINE void dtype_read_for_order_and_null_size(dtype_t* type, byte* buf)
{
	ut_ad(4 == DATA_ORDER_NULL_TYPE_BUF_SIZE);
	type->mtype = buf[0];
	type->prtype = buf[1];

	type->len = mach_read_from_2(buf + 2);
}

UNIV_INLINE ulint dtype_get_fixed_size(dtype_t* type)
{
	switch(type->mtype){
	case DATA_CHAR:
	case DATA_FIXBINARY:
	case DATA_INT:
	case DATA_FLOAT:
	case DATA_DOUBLE:
	case DATA_MYSQL:
		return dtype_get_len(type);

	case DATA_SYS:
		if(type->prtype == DATA_ROW_ID)
			return DATA_ROW_ID_LEN;
		else if(type->prtype == DATA_TRX_ID)
			return DATA_TRX_ID_LEN;
		else if(type->prtype == DATA_ROLL_PTR)
			return DATA_ROLL_PTR_LEN;
		else
			return 0;

	case DATA_VARCHAR:
	case DATA_BINARY:
	case DATA_DECIMAL:
	case DATA_VARMYSQL:
	case DATA_BLOB:
		return 0;

	default:
		ut_a(0);
	}

	return 0;
}

UNIV_INLINE ulint dtype_get_sql_null_size(dtype_t* type)
{
	return dtype_get_fixed_size(type);
}

UNIV_INLINE ibool dtype_is_fixed_size(dtype_t* type)
{
	lint size;
	size = dtype_get_fixed_size(type);
	if(size)
		return TRUE;
	
	return FALSE;
}

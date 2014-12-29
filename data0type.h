#ifndef __DATA0TYPE_H_
#define __DATA0TYPE_H_

#include "univ.h"

typedef struct dtype_struct dtype_t;

extern dtype_t*	dtype_binary;

/*MySQL的数据类型*/
#define DATA_VARCHAR		1
#define DATA_CHAR			2
#define DATA_FIXBINARY		3
#define DATA_BINARY			4
#define DATA_BLOB			5
#define DATA_INT			6
#define DATA_SYS_CHILD		7
#define DATA_SYS			8

#define DATA_FLOAT			9
#define DATA_DOUBLE			10
#define DATA_DECIMAL		11
#define DATA_VARMYSQL		12
#define DATA_MYSQL			13
#define DATA_ERROR			111
#define DATA_MTYPE_MAX		255

/****************************************************/
#define DATA_ROW_ID			0
#define DATA_ROW_ID_LEN		6
#define DATA_TRX_ID			1
#define DATA_TRX_ID_LEN		6
#define DATA_ROLL_PTR		2
#define DATA_ROLL_PTR_LEN	7
#define	DATA_MIX_ID			3
#define DATA_MIX_ID_LEN		9
#define DATA_N_SYS_COLS		4
#define DATA_NOT_NULL		256
#define DATA_UNSIGNED		512

#define DATA_ENGLISH		4
#define DATA_FINNISH		5
#define	DATA_PRTYPE_MAX		255

#define DATA_ORDER_NULL_TYPE_BUF_SIZE	4

/*****************************************************/
UNIV_INLINE void		dtype_set(dtype_t* type, ulint mtype, ulint prtype, ulint len, ulint prec);
UNIV_INLINE void		dtype_copy(dtype_t* type1, dtype_t* type2);
UNIV_INLINE ulint		dtype_get_mtype(dtype_t* type);
UNIV_INLINE ulint		dtype_get_prtype(dtype_t* type);
UNIV_INLINE ulint		dtype_get_len(dtype_t* type);
UNIV_INLINE ulint		dtype_get_prec(dtype_t* type);
UNIV_INLINE ulint		dtype_get_pad_char(dtype_t* type);
UNIV_INLINE ulint		dtype_get_fixed_size(dtype_t* type);
UNIV_INLINE ulint		dtype_get_sql_null_size(dtype_t* type);
UNIV_INLINE	ibool		dtype_is_fixed_size(dtype_t* type);
UNIV_INLINE void		dtype_store_for_order_and_null_size(byte* buf, dtype_t* type);
UNIV_INLINE void		dtype_read_for_order_and_null_size(dtype_t* type, byte* buf);
UNIV_INLINE ibool		dtype_validate(dtype_t* type);
UNIV_INLINE void		dtype_print(dtype_t* type);

struct dtype_struct
{
	ulint	mtype;
	ulint	prtype;
	ulint	len;
	ulint	prec;
};

#include "data0type.inl"

#endif





#ifndef __rem0cmp_h_
#define __rem0cmp_h_

#include "univ.h"
#include "data0data.h"
#include "data0type.h"
#include "dict0dict.h"
#include "rem0rec.h"

ibool				cmp_types_are_equal(dtype_t* type1, dtype_t* type2);

UNIV_INLINE	int		cmp_data_data(dtype_t* cur_type, byte* data1, ulint len1, byte* data2, ulint len2);

UNIV_INLINE int		cmp_dfield_dfield(dfield_t* dfield1, dfield_t* dfield2);

int					cmp_dtuple_rec_with_match(dtuple_t* dtuple, rec_t* rec, ulint* matched_fields, ulint* matched_bytes);

int					cmp_dtuple_rec(dtuple_t* dtuple, rec_t* rec);

ibool				cmp_dtuple_is_prefix_of_rec(dtuple_t* dtuple, rec_t* rec);

ibool				cmp_dtuple_rec_prefix_equal(dtuple_t* dtuple, rec_t* rec, ulint n_fields);

int					cmp_rec_rec_with_match(rec_t* rec1, rec_t* rec2, dict_index_t* index, ulint* matched_fields, ulint* matched_bytes);

UNIV_INLINE int		cmp_rec_rec(rec_t* rec1, rec_t* rec2, dict_index_t* index);

#include "rem0cmp.inl"
#endif







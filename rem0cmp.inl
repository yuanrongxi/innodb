
int cmp_data_data_slow(dtype_t* cur_type, byte* data1, ulint len1, byte* data2, ulint len2);

UNIV_INLINE int cmp_data_data(dtype_t* cur_type, byte* data1, ulint len1, byte* data2, ulint len2)
{
	return cmp_data_data_slow(cur_type, data1, len1, data2, len2);
}

UNIV_INLINE int cmp_dfield_dfield(dfield_t* dfield1, dfield_t* dfield2)
{
	ut_ad(dfield_check_typed(dfield1));
	return cmp_data_data(dfield_get_type(dfield1), dfield_get_data(dfield1), dfield_get_len(dfield1),
							dfield_get_data(dfield2), dfield_get_len(dfield2));
}

UNIV_INLINE int cmp_rec_rec(rec_t* rec1, rec_t* rec2, dict_index_t* index)
{
	ulint	match_f	= 0;
	ulint	match_b	= 0;

	return cmp_rec_rec_with_match(rec1, rec2, index, &match_f, &match_b);
}






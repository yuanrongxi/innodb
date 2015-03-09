
/*获得undo的类型值*/
UNIV_INLINE ulint trx_undo_rec_get_type(trx_undo_rec_t* undo_rec)
{
	return mach_read_from_1(undo_rec + 2) & (TRX_UNDO_CMPL_INFO_MULT - 1);
}
/*获得undo_rec的compiler info值*/
UNIV_INLINE ulint trx_undo_rec_get_cmpl_info(trx_undo_rec_t* undo_rec)
{
	return mach_read_from_1(undo_rec + 2) / TRX_UNDO_CMPL_INFO_MULT;
}

/*判断undo_rec是否包换一个extern storage field（外部存储列）*/
ibool trx_undo_rec_get_extern_storage(trx_undo_rec_t* undo_rec)
{
	if(mach_read_from_1(undo_rec + 2) & TRX_UNDO_UPD_EXTERN)
		return TRUE;

	return FALSE;
}

/*获得undo rec number*/
UNIV_INLINE dulint trx_undo_rec_get_undo_no(trx_undo_rec_t* undo_rec)
{
	byte* ptr;
	ptr = undo_rec + 3;

	return mach_dulint_read_much_compressed(ptr);
}

/*undo rec拷贝到一个heap分配的undo rec中*/
UNIV_INLINE trx_undo_rec_t* trx_undo_rec_copy(trx_undo_rec_t* undo_rec, mem_heap_t* heap)
{
	ulint			len;
	trx_undo_rec_t*	rec_copy;

	len = mach_read_from_2(undo_rec) + buf_frame_align(undo_rec) - undo_rec;
	rec_copy = mem_heap_alloc(heap, len);

	ut_memcpy(rec_copy, undo_rec, len);

	return(rec_copy);
}


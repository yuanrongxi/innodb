#define DYN_BLOCK_MAGIC_N	375767
#define DYN_BLOCK_FULL_FLAG	0x1000000


dyn_block_t* dyn_array_add_block(dyn_array_t* arr);

UNIV_INLINE dyn_block_t* dyn_array_get_first_block(dyn_array_t* arr)
{
	return arr;
}

UNIV_INLINE dyn_block_t* dyn_array_get_last_block(dyn_array_t* arr)
{
	if(arr->heap == NULL)
		return arr;

	return UT_LIST_GET_LAST(arr->base);
}

UNIV_INLINE dyn_block_t* dyn_array_get_next_block(dyn_array_t* arr, dyn_block_t* block)
{
	ut_ad(arr && block);
	if(arr->heap == NULL){
		ut_ad(arr == block);
		return NULL;
	}

	return UT_LIST_GET_NEXT(list, block);
}

UNIV_INLINE ulint dyn_block_get_used(dyn_block_t* block)
{
	ut_ad(block);
	return ((block->used) & ~DYN_BLOCK_FULL_FLAG);
}

UNIV_INLINE byte* dyn_block_get_data(dyn_block_t* block)
{
	ut_ad(block);
	return block->data;
}

UNIV_INLINE dyn_array_t* dyn_array_create(dyn_array_t* arr)
{
	ut_ad(arr);
	ut_ad(DYN_ARRAY_DATA_SIZE < DYN_BLOCK_FULL_FLAG);

	arr->heap = NULL;
	arr->used = 0;

#ifdef UNIV_DEBUG
	arr->buf_end = 0;
	arr->magic_n = DYN_BLOCK_MAGIC_N;
#endif

	return arr;
}

UNIV_INLINE void dyn_array_free(dyn_array_t* arr)
{
	if(arr->heap != NULL)
		mem_heap_free(arr->heap);

#ifdef UNIV_DEBUG
	arr->magic_n = 0;
#endif
}

UNIV_INLINE void* dyn_array_push(dyn_array_t* arr, ulint size)
{
	dyn_block_t*	block;
	ulint			used;

	ut_ad(arr);
	ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);
	ut_ad(size <= DYN_ARRAY_DATA_SIZE);
	ut_ad(size);

	block = arr;
	used = block->used;

	if (used + size > DYN_ARRAY_DATA_SIZE) {
		/* Get the last array block */
		block = dyn_array_get_last_block(arr);
		used = block->used;

		if (used + size > DYN_ARRAY_DATA_SIZE) {
			block = dyn_array_add_block(arr);
			used = block->used;
		}
	}

	block->used = used + size;
	ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);

	return((block->data) + used);
}

UNIV_INLINE byte* dyn_array_open(dyn_array_t* arr, ulint size)
{
	dyn_block_t*	block;
	ulint			used;

	ut_ad(arr);
	ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);
	ut_ad(size <= DYN_ARRAY_DATA_SIZE);
	ut_ad(size);

	block = arr;
	used = block->used;

	if (used + size > DYN_ARRAY_DATA_SIZE) {
		/* Get the last array block */
		block = dyn_array_get_last_block(arr);
		used = block->used;

		if (used + size > DYN_ARRAY_DATA_SIZE) {
			block = dyn_array_add_block(arr);
			used = block->used;
			ut_a(size <= DYN_ARRAY_DATA_SIZE);
		}
	}

	ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);
#ifdef UNIV_DEBUG
	ut_ad(arr->buf_end == 0);
	arr->buf_end = user + size;
#endif
	return ((block->data) + used);
}

UNIV_INLINE void dyn_array_close(dyn_array_t* arr, byte* ptr)
{
	dyn_block_t*	block;

	ut_ad(arr);
	ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

	block = dyn_array_get_last_block(arr);
	ut_ad(arr->buf_end + block->data >= ptr);

	block->used = ptr - block->data;
	ut_ad(block->used <= DYN_ARRAY_DATA_SIZE);

#ifdef UNIV_DEBUG
	arr->buf_end = 0;
#endif
}

UNIV_INLINE void* dyn_array_get_element(dyn_array_t* arr, ulint pos)
{
	dyn_block_t* arr;
	ulint used;

	ut_ad(arr);
	ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

	block = dyn_array_get_first_block(arr);
	if(arr->heap != NULL){
		used = dyn_block_get_used(arr);
		while(pos >= used){
			pos -= used;
			block = UT_LIST_GET_NEXT(list, block);
			ut_ad(block);

			used = dyn_block_get_used(block);
		}
	}

	ut_ad(block);
	ut_ad(dyn_block_get_used(block) >= pos);

	return block->data + pos;
}

UNIV_INLINE ulint dyn_array_get_data_size(dyn_array_t* arr)
{
	dyn_block_t*	block;
	ulint			sum = 0;

	ut_ad(arr);
	ut_ad(arr->magic_n == DYN_BLOCK_MAGIC_N);

	if(arr->heap == NULL)
		return arr->used;

	block = dyn_array_get_first_block(arr);
	while(block != NULL){
		sum += dyn_block_get_used(block);
		block = dyn_array_get_next_block(arr, block);
	}

	return sum;
}

UNIV_INLINE void dyn_push_string(dyn_array_t* arr, byte* str, ulint len)
{
	byte*	ptr;
	ulint	n_copied;

	while(len > 0){
		if(len > DYN_ARRAY_DATA_SIZE)
			n_copied = DYN_ARRAY_DATA_SIZE;
		else
			n_copied = len;
	}

	ptr = (byte *)dyn_array_push(arr, n_copied);

	ut_memcpy(ptr, str, n_copied);
	str += n_copied;
	len -= n_copied;
}







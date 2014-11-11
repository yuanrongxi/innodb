/******************************************************
The memory management: the debug code. This is not a compilation module,
but is included in mem0mem.* !

(c) 1994, 1995 Innobase Oy

Created 6/9/1994 Heikki Tuuri
*******************************************************/

/* In the debug version each allocated field is surrounded with
check fields whose sizes are given below */

#ifdef UNIV_MEM_DEBUG
#define MEM_FIELD_HEADER_SIZE   ut_calc_align(2 * sizeof(ulint),\
						UNIV_MEM_ALIGNMENT)
#define MEM_FIELD_TRAILER_SIZE  sizeof(ulint)
#else
#define MEM_FIELD_HEADER_SIZE   0
#endif


/* Space needed when allocating for a user a field of
length N. The space is allocated only in multiples of
UNIV_MEM_ALIGNMENT. In the debug version there are also
check fields at the both ends of the field. */
#ifdef UNIV_MEM_DEBUG
#define MEM_SPACE_NEEDED(N) ut_calc_align((N) + MEM_FIELD_HEADER_SIZE\
			       	              + MEM_FIELD_TRAILER_SIZE,\
				          UNIV_MEM_ALIGNMENT)
#else
#define MEM_SPACE_NEEDED(N) ut_calc_align((N), UNIV_MEM_ALIGNMENT)
#endif

/*******************************************************************
Checks a memory heap for consistency and prints the contents if requested.
Outputs the sum of sizes of buffers given to the user (only in
the debug version), the physical size of the heap and the number of
blocks in the heap. In case of error returns 0 as sizes and number
of blocks. */

void
mem_heap_validate_or_print(
/*=======================*/
	mem_heap_t*   	heap, 	/* in: memory heap */
	byte*		top,	/* in: calculate and validate only until
				this top pointer in the heap is reached,
				if this pointer is NULL, ignored */
	ibool            print,  /* in: if TRUE, prints the contents
				of the heap; works only in
				the debug version */
	ibool*           error,  /* out: TRUE if error */
	ulint*          us_size,/* out: allocated memory 
				(for the user) in the heap,
				if a NULL pointer is passed as this
				argument, it is ignored; in the
				non-debug version this is always -1 */
	ulint*          ph_size,/* out: physical size of the heap,
				if a NULL pointer is passed as this
				argument, it is ignored */
	ulint*          n_blocks); /* out: number of blocks in the heap,
				if a NULL pointer is passed as this
				argument, it is ignored */
/******************************************************************
Prints the contents of a memory heap. */

void
mem_heap_print(
/*===========*/
	mem_heap_t*   heap);	/* in: memory heap */
/******************************************************************
Checks that an object is a memory heap (or a block of it) */

ibool
mem_heap_check(
/*===========*/
				/* out: TRUE if ok */
	mem_heap_t*   heap);	/* in: memory heap */
/******************************************************************
Validates the contents of a memory heap. */

ibool
mem_heap_validate(
/*==============*/
				/* out: TRUE if ok */
	mem_heap_t*   heap);	/* in: memory heap */
/*********************************************************************
Prints information of dynamic memory usage and currently live
memory heaps or buffers. Can only be used in the debug version. */

void
mem_print_info(void);
/*=================*/
/*********************************************************************
Prints information of dynamic memory usage and currently allocated memory
heaps or buffers since the last ..._print_info or..._print_new_info. */

void
mem_print_new_info(void);
/*====================*/
/*********************************************************************
TRUE if no memory is currently allocated. */

ibool
mem_all_freed(void);
/*===============*/
			/* out: TRUE if no heaps exist */
/*********************************************************************
Validates the dynamic memory */

ibool
mem_validate_no_assert(void);
/*=========================*/
			/* out: TRUE if error */
/****************************************************************
Validates the dynamic memory */

ibool
mem_validate(void);
/*===============*/
			/* out: TRUE if ok */
/****************************************************************
Tries to find neigboring memory allocation blocks and dumps to stderr
the neighborhood of a given pointer. */

void
mem_analyze_corruption(
/*===================*/
	byte*	ptr);	/* in: pointer to place of possible corruption */


extern mutex_t	mem_hash_mutex;
extern ulint	mem_current_allocated_memory;

/**********************************************************************
Initializes an allocated memory field in the debug version. */

void
	mem_field_init(
	/*===========*/
	byte*	buf,	/* in: memory field */
	ulint	n);	/* in: how many bytes the user requested */
/**********************************************************************
Erases an allocated memory field in the debug version. */

void
	mem_field_erase(
	/*============*/
	byte*	buf,	/* in: memory field */
	ulint	n);	/* in: how many bytes the user requested */
/*******************************************************************
Initializes a buffer to a random combination of hex BA and BE.
Used to initialize allocated memory. */

void
	mem_init_buf(
	/*=========*/
	byte*   buf,    /* in: pointer to buffer */
	ulint    n);     /* in: length of buffer */
/*******************************************************************
Initializes a buffer to a random combination of hex DE and AD.
Used to erase freed memory.*/

void
	mem_erase_buf(
	/*==========*/
	byte*   buf,    /* in: pointer to buffer */
	ulint    n);     /* in: length of buffer */
/*******************************************************************
Inserts a created memory heap to the hash table of
current allocated memory heaps.
Initializes the hash table when first called. */

void
	mem_hash_insert(
	/*============*/
	mem_heap_t*	heap,	   /* in: the created heap */
	char*		file_name, /* in: file name of creation */
	ulint		line);	   /* in: line where created */
/*******************************************************************
Removes a memory heap (which is going to be freed by the caller)
from the list of live memory heaps. Returns the size of the heap
in terms of how much memory in bytes was allocated for the user of
the heap (not the total space occupied by the heap).
Also validates the heap.
NOTE: This function does not free the storage occupied by the
heap itself, only the node in the list of heaps. */

void
	mem_hash_remove(
	/*============*/
	mem_heap_t*	heap,	   /* in: the heap to be freed */
	char*		file_name, /* in: file name of freeing */
	ulint		line);	   /* in: line where freed */


void
	mem_field_header_set_len(byte* field, ulint len);

ulint
	mem_field_header_get_len(byte* field);

void
	mem_field_header_set_check(byte* field, ulint check);

ulint
	mem_field_header_get_check(byte* field);

void
	mem_field_trailer_set_check(byte* field, ulint check);

ulint
	mem_field_trailer_get_check(byte* field);


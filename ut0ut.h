#ifndef ut0ut_h
#define ut0ut_h

#include "univ.h"

#ifndef UNIV_INNOCHECKSUM

#include "db0err.h"

#ifndef UNIV_HOTBACKUP
# include "os0sync.h" /* for HAVE_ATOMIC_BUILTINS */
#endif /* UNIV_HOTBACKUP */

#include <time.h>

#ifndef MYSQL_SERVER
#include <ctype.h>
#endif

#include <stdarg.h>

#define TEMP_INDEX_PREFIX '\377'
#define TEMP_INDEX_PREFIX_STR	"\377"

/*Time stamp*/
typedef time_t ib_time_t;

template<typename F>
struct ut_when_dtor
{
	ut_when_dtor(F& p) : f(p){};
	~ut_when_dtor(){
		f();
	};

private:
	F& f;
};

#ifndef UNIV_HOTBACKUP
# if defined(HAVE_PAUSE_INSTRUCTION)
   /* According to the gcc info page, asm volatile means that the
   instruction has important side-effects and must not be removed.
   Also asm volatile may trigger a memory barrier (spilling all registers
   to memory). */
#  ifdef __SUNPRO_CC
#   define UT_RELAX_CPU() asm ("pause" )
#  else
#   define UT_RELAX_CPU() __asm__ __volatile__ ("pause")
#  endif /* __SUNPRO_CC */

# elif defined(HAVE_FAKE_PAUSE_INSTRUCTION)
#  define UT_RELAX_CPU() __asm__ __volatile__ ("rep; nop")
# elif defined(HAVE_ATOMIC_BUILTINS)
#  define UT_RELAX_CPU() do { \
     volatile lint	volatile_var; \
     os_compare_and_swap_lint(&volatile_var, 0, 1); \
   } while (0)
# elif defined(HAVE_WINDOWS_ATOMICS)
   /* In the Win32 API, the x86 PAUSE instruction is executed by calling
   the YieldProcessor macro defined in WinNT.h. It is a CPU architecture-
   independent way by using YieldProcessor. */
#  define UT_RELAX_CPU() YieldProcessor()
# else
#  define UT_RELAX_CPU() ((void)0) /* avoid warning for an empty statement */
# endif

/*********************************************************************//**
Delays execution for at most max_wait_us microseconds or returns earlier
if cond becomes true.
@param cond		in: condition to wait for; evaluated every 2 ms
@param max_wait_us	in: maximum delay to wait, in microseconds */
#define UT_WAIT_FOR(cond, max_wait_us)				\
do {								\
	ullint	start_us;					\
	start_us = ut_time_us(NULL);				\
	while (!(cond) 						\
	       && ut_time_us(NULL) - start_us < (max_wait_us)) {\
								\
		os_thread_sleep(2000 /* 2 ms */);		\
	}							\
} while (0)
#endif /* !UNIV_HOTBACKUP */

/*取大小函数*/
template <class T> T ut_min(T a, T b) { return(a < b ? a : b); }
template <class T> T ut_max(T a, T b) { return(a > b ? a : b); }

UNIV_INLINE ulint ut_min(ulint n1, ulint n2);
UNIV_INLINE ulint ut_max(ulint n1, ulint n2);

/*获取最小的一组值*/
UNIV_INLINE ulint ut_pair_min(ulint* a, ulint* b, ulint a1, ulint b1, ulint a2, ulint b2);

UNIV_INLINE int ut_ulint_cmp(ulint a, ulint b);

UNIV_INLINE int ut_pair_cmp(ulint a1, ulint a2, ulint b1, ulint b2);

/*判断N是否是2的次方数*/
#define ut_is_2pow(n) UNIV_LIKELY(!((n) & ~((n) - 1)))
/*2的次方数为单位取余*/
#define ut_2pow_remainder(n, m) ((n) & ((m) - 1))
/*和ut_uint64_algin_down相同*/
#define ut_2pow_round(n, m) ((n) & ~((m) - 1))
#define ut_calc_align_down(n, m) ut_2pow_round(n, m)
/*和ut_uint64_algin_up相同*/
#define ut_calc_align(n, m) (((n) + ((m) - 1)) & ~((m) - 1))

UNIV_INLINE ulint ut_2_log(ulint n);
UNIV_INLINE ulint ut_2_exp(ulint n);

/*获得离n最近的2的次方数，这个数必须小于n*/
UNIV_INLINE ulint ut_2_power_up(ulint n) __attribute__(const);

#define UT_BITS_IN_BYTES(b) (((b) + 7) / 8)

UNIV_INLINE ib_time_t ut_time(void);

#ifndef UNIV_HOTBACKUP
UNIV_INTERN int ut_usectime(ulint* sec, ulint* ms);
UNIV_INTERN ullint ut_time_us(ullint* tloc);
UNIV_INTERN ulint ut_time_ms(void);
#endif
UNIV_INTERN double ut_difftime(ib_time_t time2, ib_time_t time1);
#endif /* !UNIV_INNOCHECKSUM */

UNIV_INTERN void ut_print_timestamp(FILE* file) UNIV_COLD __attribute__((nonnull));

#ifndef UNIV_INNOCHECKSUM
/**********************************************************//**
Sprintfs a timestamp to a buffer, 13..14 chars plus terminating NUL. */
UNIV_INTERN
void
ut_sprintf_timestamp(
/*=================*/
	char*	buf); /*!< in: buffer where to sprintf */
#ifdef UNIV_HOTBACKUP
/**********************************************************//**
Sprintfs a timestamp to a buffer with no spaces and with ':' characters
replaced by '_'. */
UNIV_INTERN
void
ut_sprintf_timestamp_without_extra_chars(
/*=====================================*/
	char*	buf); /*!< in: buffer where to sprintf */
/**********************************************************//**
Returns current year, month, day. */
UNIV_INTERN
void
ut_get_year_month_day(
/*==================*/
	ulint*	year,	/*!< out: current year */
	ulint*	month,	/*!< out: month */
	ulint*	day);	/*!< out: day */
#else /* UNIV_HOTBACKUP */
/*************************************************************//**
Runs an idle loop on CPU. The argument gives the desired delay
in microseconds on 100 MHz Pentium + Visual C++.
@return	dummy value */
UNIV_INTERN
ulint
ut_delay(
/*=====*/
	ulint	delay);	/*!< in: delay in microseconds on 100 MHz Pentium */
#endif /* UNIV_HOTBACKUP */
/*************************************************************//**
Prints the contents of a memory buffer in hex and ascii. */
UNIV_INTERN
void
ut_print_buf(
/*=========*/
	FILE*		file,	/*!< in: file where to print */
	const void*	buf,	/*!< in: memory buffer */
	ulint		len);	/*!< in: length of the buffer */

/**********************************************************************//**
Outputs a NUL-terminated file name, quoted with apostrophes. */
UNIV_INTERN
void
ut_print_filename(
/*==============*/
	FILE*		f,	/*!< in: output stream */
	const char*	name);	/*!< in: name to print */

#ifndef UNIV_HOTBACKUP
/* Forward declaration of transaction handle */
struct trx_t;

/**********************************************************************//**
Outputs a fixed-length string, quoted as an SQL identifier.
If the string contains a slash '/', the string will be
output as two identifiers separated by a period (.),
as in SQL database_name.identifier. */
UNIV_INTERN
void
ut_print_name(
/*==========*/
	FILE*		f,	/*!< in: output stream */
	const trx_t*	trx,	/*!< in: transaction */
	ibool		table_id,/*!< in: TRUE=print a table name,
				FALSE=print other identifier */
	const char*	name);	/*!< in: name to print */

/**********************************************************************//**
Outputs a fixed-length string, quoted as an SQL identifier.
If the string contains a slash '/', the string will be
output as two identifiers separated by a period (.),
as in SQL database_name.identifier. */
UNIV_INTERN
void
ut_print_namel(
/*===========*/
	FILE*		f,	/*!< in: output stream */
	const trx_t*	trx,	/*!< in: transaction (NULL=no quotes) */
	ibool		table_id,/*!< in: TRUE=print a table name,
				FALSE=print other identifier */
	const char*	name,	/*!< in: name to print */
	ulint		namelen);/*!< in: length of name */

/**********************************************************************//**
Formats a table or index name, quoted as an SQL identifier. If the name
contains a slash '/', the result will contain two identifiers separated by
a period (.), as in SQL database_name.identifier.
@return pointer to 'formatted' */
UNIV_INTERN
char*
ut_format_name(
/*===========*/
	const char*	name,		/*!< in: table or index name, must be
					'\0'-terminated */
	ibool		is_table,	/*!< in: if TRUE then 'name' is a table
					name */
	char*		formatted,	/*!< out: formatted result, will be
					'\0'-terminated */
	ulint		formatted_size);/*!< out: no more than this number of
					bytes will be written to 'formatted' */

/**********************************************************************//**
Catenate files. */
UNIV_INTERN
void
ut_copy_file(
/*=========*/
	FILE*	dest,	/*!< in: output file */
	FILE*	src);	/*!< in: input file to be appended to output */
#endif /* !UNIV_HOTBACKUP */

#ifdef __WIN__
/**********************************************************************//**
A substitute for vsnprintf(3), formatted output conversion into
a limited buffer. Note: this function DOES NOT return the number of
characters that would have been printed if the buffer was unlimited because
VC's _vsnprintf() returns -1 in this case and we would need to call
_vscprintf() in addition to estimate that but we would need another copy
of "ap" for that and VC does not provide va_copy(). */
UNIV_INTERN
void
ut_vsnprintf(
/*=========*/
	char*		str,	/*!< out: string */
	size_t		size,	/*!< in: str size */
	const char*	fmt,	/*!< in: format */
	va_list		ap);	/*!< in: format values */

/**********************************************************************//**
A substitute for snprintf(3), formatted output conversion into
a limited buffer.
@return number of characters that would have been printed if the size
were unlimited, not including the terminating '\0'. */
UNIV_INTERN
int
ut_snprintf(
/*========*/
	char*		str,	/*!< out: string */
	size_t		size,	/*!< in: str size */
	const char*	fmt,	/*!< in: format */
	...);			/*!< in: format values */
#else
/**********************************************************************//**
A wrapper for vsnprintf(3), formatted output conversion into
a limited buffer. Note: this function DOES NOT return the number of
characters that would have been printed if the buffer was unlimited because
VC's _vsnprintf() returns -1 in this case and we would need to call
_vscprintf() in addition to estimate that but we would need another copy
of "ap" for that and VC does not provide va_copy(). */
# define ut_vsnprintf(buf, size, fmt, ap)	\
	((void) vsnprintf(buf, size, fmt, ap))
/**********************************************************************//**
A wrapper for snprintf(3), formatted output conversion into
a limited buffer. */
# define ut_snprintf	snprintf
#endif /* __WIN__ */

/*************************************************************//**
Convert an error number to a human readable text message. The
returned string is static and should not be freed or modified.
@return	string, describing the error */
UNIV_INTERN
const char*
ut_strerr(
/*======*/
	dberr_t	num);	/*!< in: error number */

/****************************************************************
Sort function for ulint arrays. */
UNIV_INTERN
void
ut_ulint_sort(
/*==========*/
	ulint*	arr,		/*!< in/out: array to sort */
	ulint*	aux_arr,	/*!< in/out: aux array to use in sort */
	ulint	low,		/*!< in: lower bound */
	ulint	high)		/*!< in: upper bound */
	__attribute__((nonnull));
#endif

#endif

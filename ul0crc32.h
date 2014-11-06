#ifndef ut0crc32_h
#define ut0crc32_h

#include "univ.h"

UNIV_INTERNvoid ut_crc32_init();

typedef ib_uint32_t (*ib_ut_crc32_t)(const byte* ptr, ulint len);

extern ib_ut_crc32_t ut_crc32;

extern bool	ut_crc32_sse2_enabled;

#endif






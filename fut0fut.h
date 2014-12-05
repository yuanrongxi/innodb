#ifndef __FUT0FUT_H_
#define __FUT0FUT_H_

#include "univ.h"
#include "fil0fil.h"
#include "mtr0mtr.h"

UNIV_INLINE byte* fut_get_ptr(ulint space, fil_addr_t addr, ulint rw_latch, mtr_t* mtr);

#endif




#ifndef __row0umod_h_
#define __row0umod_h_

#include "univ.h"
#include "data0data.h"
#include "dict0types.h"
#include "trx0types.h"
#include "que0types.h"
#include "row0types.h"
#include "mtr0mtr.h"

ulint row_undo_mod(undo_node_t* node, que_thr_t* thr);

#endif




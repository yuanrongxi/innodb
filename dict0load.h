#ifndef __dict0load_h_
#define __dict0load_h_

#include "univ.h"
#include "dict0types.h"
#include "ut0byte.h"

char*					dict_get_first_table_name_in_db(char* name);

dict_table_t*			dict_load_table(char* name);

dict_table_t*			dict_load_table_on_id(dulint table_id);

void					dict_load_sys_table(dict_table_t* table);

ulint					dict_load_foreigns(char* table_name);

void					dict_print();

#endif







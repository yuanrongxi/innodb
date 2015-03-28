#ifndef __srv0start_h_
#define __srv0start_h_

#include "univ.h"

#define SRV_SHUTDOWN_CLEANUP		1
#define SRV_SHUTDOWN_LAST_PHASE		2

void			srv_normalize_path_for_win(char* str);

char*			srv_add_path_separator_if_needed(char* str);

ibool			srv_parse_data_file_paths_and_sizes(char* str, char*** data_file_names, ulint** data_file_sizes, ulint** data_file_is_raw_partition
											ulint* n_data_files, ibool* is_auto_extending, ulint* max_auto_extend_size);

ibool			srv_parse_log_group_home_dirs(char* str, char*** log_group_home_dirs);

int				innobase_start_or_create_for_mysql();

int				innobase_shutdown_for_mysql();


extern	ibool	srv_startup_is_before_trx_rollback_phase;
extern	ibool	srv_is_being_shut_down;

#endif






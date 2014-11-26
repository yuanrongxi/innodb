#include "sync0sync.h"
#include "mem0mem.h"
#include "log0log.h"
#include "os0file.h"

extern ibool recv_recovery_from_backup_on;

UNIV_INLINE ibool recv_recovery_is_on()
{
	return recv_recovery_on;
}

UNIV_INLINE ibool recv_recovery_from_backup_is_on()
{
	return recv_recovery_from_backup_on;
}


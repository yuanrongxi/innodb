#ifndef __fut0lst_h_
#define __fut0lst_h_

#include "univ.h"

#include "fil0fil.h"
#include "mtr0mtr.h"

typedef byte		flst_base_node_t;
typedef byte		flst_node_t;

#define FLST_BASE_NODE_SIZE		(4 + 2 * FIL_ADDR_SIZE)
#define FLST_NODE_SIZE			(2 * FIL_ADDR_SIZE)

/*磁盘上的单向链表*/

/*初始化一个磁盘链表*/
UNIV_INLINE void			flst_init(flst_base_node_t* base, mtr_t* mtr);
/*获得链表单元个数*/
UNIV_INLINE ulint			flst_get_len(flst_base_node_t* );
/*获得链表的第一个node地址*/
UNIV_INLINE fil_addr_t		flst_get_first(flst_base_node_t* base, mtr_t* mtr);
/*获得链表最后一个node地址*/
UNIV_INLINE fil_addr_t		flst_get_last(flst_base_node_t* base, mtr_t* mtr);
/*获得node下一个单元的地址*/
UNIV_INLINE	fil_addr_t		flst_get_next_addr(flst_base_node_t* node, mtr_t* mtr);
/*获得node上一个单元的地址*/
UNIV_INLINE fil_addr_t		flst_get_prev_addr(flst_base_node_t* node, mtr_t* mtr);
/*修改node对应的地址*/
UNIV_INLINE void			flst_write_addr(fil_faddr_t* faddr, fil_addr_t addr, mtr_t* mtr);
/*从node读取对应的地址*/
UNIV_INLINE fil_addr_t		flst_read_addr(fil_faddr_t* faddr, mtr_t* mtr);
/*将node插入到链表的最后*/
void						flst_add_last(flst_base_node_t* base, flst_node_t* node, mtr_t* mtr);
/*将链表插入到链表的前面*/
void						flst_add_first(flst_base_node_t* base, flst_node_t* node, mtr_t* mtr);
/*在node1的后面插入node2*/
void						flst_insert_after(flst_base_node_t* base, flst_node_t* node1, flst_node_t* node2, mtr_t* mtr);
/*在node3的前面插入node2*/
void						flst_insert_before(flst_base_node_t* base, flst_node_t* node2, flst_node_t* node3, mtr_t* mtr);
/*删除链表的node2*/
void						flst_remove(flst_base_node_t* base, flst_node_t* node2, mtr_t* mtr);
/*删除掉从node2之后的所有节点*/
void						flst_cur_end(flst_base_node_t* base, flst_node_t* node2, ulint n_nodes, mtr_t* mtr);

void						flst_truncate_end(flst_base_node_t* base, flst_node_t* node2, ulint n_nodes, mtr_t* mtr);

ibool						flst_validate(flst_base_node_t* base, mtr_t* mtr1);

void						flst_print(flst_base_node_t* base, mtr_t* mtr);

#include "fut0lst.inl"

#endif





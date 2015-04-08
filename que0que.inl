#include "usr0sess.h"

/*获得thr对应的事务对象*/
UNIV_INLINE trx_t* trx_get_trx(que_thr_t* thr)
{
	ut_ad(thr);

	return thr->graph->trx;
}

/*获得fork的第一个thr*/
UNIV_INLINE que_thr_t* que_fork_get_first_thr(que_fork_t* fork)
{
	return UT_LIST_GET_FIRST(fork->thrs);
}

/*获得fork第一个thr的孩子节点*/
UNIV_INLINE que_node_t* que_fork_get_child(que_fork_t* fork)
{
	que_thr_t* thr;

	thr = UT_LIST_GET_FIRST(fork->thrs);
	return thr->child;
}

/*获得node的类型*/
UNIV_INLINE ulint que_node_get_type(que_node_t* node)
{
	ut_ad(node);

	return ((que_common_t *)node)->type;
}

/*获得node的val field*/
UNIV_INLINE dfield_t* que_node_get_val(que_node_t*	node)
{
	ut_ad(node);
	return(&(((que_common_t*)node)->val));
}
/*获得node的val field size*/
UNIV_INLINE ulint que_node_get_val_buf_size(que_node_t*	node)
{
	ut_ad(node);
	return(((que_common_t*)node)->val_buf_size);
}
/*设置node的val field size*/
UNIV_INLINE void que_node_set_val_buf_size(que_node_t*	node, ulint	 size)
{
	ut_ad(node);
	((que_common_t*)node)->val_buf_size = size;
}

/*设置node的父亲节点*/
UNIV_INLINE void que_node_set_parent(que_node_t* node, que_node_t* parent)
{
	ut_ad(node);
	((que_common_t *)node)->parent = parent;
}

/*获得node->val的列类型*/
UNIV_INLINE dtype_t* que_node_get_data_type(que_node_t* node)
{
	ut_ad(node);
	return (&(((que_common_t*)node)->val.type));
}

/*将node加入到node_list的最后面*/
UNIV_INLINE que_node_t* que_node_list_add_last(que_node_t* node_list, que_node_t* node)
{
	que_common_t*	cnode;
	que_common_t*	cnode2;

	cnode = (que_common_t*)node;
	cnode->brother = NULL;

	if(node_list == NULL)
		return node;

	cnode2 = node_list;

	while(cnode2->brother != NULL)
		cnode2 = cnode2->brother;

	cnode2->brother = node;
}

/*获得node的后面一个兄弟节点*/
UNIV_INLINE que_node_t* que_node_get_next(que_node_t* node)
{
	return (((que_common_t*)node)->brother);
}

/*获得node_list中的节点数量*/
UNIV_INLINE ulint que_node_list_get_len(que_node_t* node_list)
{
	que_common_t* cnode;
	ulint len;

	cnode = node_list;
	len = 0;

	while(cnode != NULL){
		len ++;
		cnode = cnode->brother;
	}

	return len;
}

/*获取node的父亲节点*/
UNIV_INLINE que_node_t* que_node_get_parent(que_node_t* node)
{
	return(((que_common_t*)node)->parent);
}

/*这个函数是做啥用的？*/
UNIV_INLINE ibool que_thr_peek_stop(que_thr_t* thr)
{
	trx_t*	trx;
	que_t*	graph;

	graph = thr->graph;
	trx = graph->trx;

	if(graph->state != QUE_FORK_ACTIVE || trx->que_state == TRX_QUE_LOCK_WAIT
		|| (UT_LIST_GET_LEN(trx->signals) > 0 && trx->que_state == TRX_QUE_RUNNING))
		return TRUE;

	return FALSE;
}

/*判断query graph是否是为select服务*/
UNIV_INLINE ibool que_graph_is_select(que_t* graph)
{
	if (graph->fork_type == QUE_FORK_SELECT_SCROLL || graph->fork_type == QUE_FORK_SELECT_NON_SCROLL)
		return TRUE;

	return FALSE;
}


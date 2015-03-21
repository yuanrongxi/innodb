/****************************************************
*Query graph相关的类型定义
****************************************************/
#ifndef __que0types_h_
#define __que0types_h_

typedef void						que_node_t;
typedef struct que_fork_struct		que_fork_t;
typedef que_fork_t					que_t;

typedef struct que_thr_struct		que_thr_t;
typedef struct que_common_struct	que_common_t;

struct que_common_struct
{
	ulint			type;			/*query node类型*/
	que_node_t*		parent;			/*返回父节点的pointer*/
	que_node_t*		brother;		/*兄弟节点的pointer*/
	dfield_t		val;			
	ulint			val_buf_size;
};

#endif





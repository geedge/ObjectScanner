#ifndef	__ID_NAME_INTERFACE_H__
#define	__ID_NAME_INTERFACE_H__

#ifdef __cplusplus
extern "C"{
#endif

typedef void *(*pMalloc)(int);
typedef void (*pFree)(void*);
typedef void *(*pRealloc)(void*,int);
typedef int (*pSize)(void*);
typedef int (*pRoundup)(int);
typedef int (*pInit)(void*);
typedef void (*pShutdown)(void*);

typedef struct _nt_sys_fn
{
	pMalloc    nt_malloc;
	pFree      nt_free;
	pRealloc   nt_realloc;
	pSize      nt_size;
	pRoundup   nt_roundup;
	pInit      nt_init;
	pShutdown  nt_shutdown;
	void       *param;
} NT_SYS_FN, *P_NT_SYS_FN;

long AVL_NTranser_Init(const char *path, void **handle);

long AVL_NTranser_Init_Ex(const char *path, P_NT_SYS_FN sys_fn, void **handle);

long AVL_NTranser_QueryNameByID(void *handle, const char *mod_name, long id, unsigned char *buf, unsigned long size);

void AVL_NTranser_Release(void *handle);

#ifdef __cplusplus
}
#endif

#endif // __ID_NAME_INTERFACE_H__



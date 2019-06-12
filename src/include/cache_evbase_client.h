#ifndef __CACHE_ASYN_CLIENT_H__
#define __CACHE_ASYN_CLIENT_H__

#include <event2/event.h>  
#include <event.h>

#include "tango_cache_client.h"

/* API的使用说明参考tango_cache_client.h */

struct cache_evbase_instance
{
	struct tango_cache_instance *instance;
	evutil_socket_t notify_readfd;
	evutil_socket_t notify_sendfd;
	struct event_base* evbase;
};

struct cache_evbase_ctx
{
	size_t object_size; //tango_ctx的这个成员无法返回过来，直接获取的话存在读写一致性问题
	struct tango_cache_ctx *ctx;
	struct tango_losf_meta *losf_meta; //LOSF一次性上传时没有ctx
	struct cache_evbase_instance *instance_asyn;
};

/*所有API线程安全，但是同一个cache_evbase_ctx不可以跨线程访问。*/

enum CACHE_ERR_CODE cache_evbase_get_last_error(const struct cache_evbase_ctx *ctx_asyn);
enum CACHE_ERR_CODE cache_evbase_ctx_error(const struct cache_evbase_instance *instance);
long long cache_evbase_maximum_session(const struct cache_evbase_instance *instance);
void cache_evbase_get_statistics(const struct cache_evbase_instance *instance, struct cache_statistics *out);

void cache_evbase_global_init(void);

//每个minio集群和bucket创建一个parameter，多个instance可共享一个parameter；
struct tango_cache_parameter *cache_evbase_parameter_new(const char* profile_path, const char* section, void *runtimelog);

/*创建实例，线程安全，内部会启动一个线程*/
struct cache_evbase_instance *cache_evbase_instance_new(struct tango_cache_parameter *param, void *runtimelog);

/******************************************* GET接口 ****************************************/
//成功返回0，失败返回-1，future回调函数会在另外的线程中执行，下同
int cache_evbase_fetch_object(struct cache_evbase_instance *instance, struct future* f, struct tango_cache_meta_get *meta, enum OBJECT_LOCATION where_to_get);
int cache_evbase_head_object(struct cache_evbase_instance *instance, struct future* f, struct tango_cache_meta_get *meta);
struct tango_cache_result *cache_evbase_read_result(void *promise_result);

/****************************************** DELETE接口 **************************************/
int cache_evbase_delete_object(struct cache_evbase_instance *instance, struct future* f, const char *objkey, const char *minio_addr=NULL, const char *bucket=NULL);

/***************************************** 一次性上传接口 **********************************/
int cache_evbase_upload_once_data(struct cache_evbase_instance *instance, struct future* f, 
                                               enum PUT_MEMORY_COPY_WAY way, const char *data, size_t size, 
                                               struct tango_cache_meta_put *meta, 
                                               char *path/*OUT*/, size_t pathsize);
int cache_evbase_upload_once_evbuf(struct cache_evbase_instance *instance, struct future* f, 
                                                struct evbuffer *evbuf, 
                                                struct tango_cache_meta_put *meta, 
                                                char *path/*OUT*/, size_t pathsize);

/****************************************** 流式上传接口 ***********************************/
struct cache_evbase_ctx *cache_evbase_update_start(struct cache_evbase_instance *instance, struct future* f, struct tango_cache_meta_put *meta);
int cache_evbase_update_frag_data(struct cache_evbase_ctx *ctx_asyn, enum PUT_MEMORY_COPY_WAY way, const char *data, size_t size);
int cache_evbase_update_frag_evbuf(struct cache_evbase_ctx *ctx_asyn, struct evbuffer *evbuf);
int cache_evbase_update_end(struct cache_evbase_ctx *ctx_asyn, char *path/*OUT*/, size_t pathsize);
void cache_evbase_update_cancel(struct cache_evbase_ctx *ctx_asyn);

#endif


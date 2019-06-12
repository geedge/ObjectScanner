#ifndef __OBJECT_STORE_CLIENT_H__
#define __OBJECT_STORE_CLIENT_H__

#include <event2/event.h>  
#include <event.h>

#include "cache_evbase_client.h"

struct object_store_instance
{
	struct cache_evbase_instance **instances;
	u_int32_t instance_num;
};

/*所有API线程安全，API的使用说明参考tango_cache_client.h*/

enum CACHE_ERR_CODE object_store_get_last_error(const struct cache_evbase_ctx *ctx_asyn);
long long object_store_maximum_session(const struct object_store_instance *instance);
void object_store_get_statistics(const struct object_store_instance *instance, struct cache_statistics *out);

void object_store_global_init(void);

/*创建实例，线程安全，内部会启动一个线程*/
struct object_store_instance *object_store_instance_new(const char* profile_path, const char* section, int thread_num, void *runtimelog);


//GET接口，成功返回0，失败返回-1，future回调函数会在另外的线程中执行，下同
int object_store_fetch_object(struct object_store_instance *instance, struct future* f, struct tango_cache_meta_get *meta, enum OBJECT_LOCATION where_to_get);
int object_store_head_object(struct object_store_instance *instance, struct future* f, struct tango_cache_meta_get *meta);
struct tango_cache_result *object_store_read_result(void *promise_result);

//DELETE接口
int object_store_delete_object(struct object_store_instance *instance, struct future* f, const char *objkey, const char *minio_addr=NULL, const char *bucket=NULL);

//一次性上传接口
int object_store_upload_once_data(struct object_store_instance *instance, struct future* f, 
                                               enum PUT_MEMORY_COPY_WAY way, const char *data, size_t size, 
                                               struct tango_cache_meta_put *meta, 
                                               char *path/*OUT*/, size_t pathsize);
int object_store_upload_once_evbuf(struct object_store_instance *instance, struct future* f, 
                                                struct evbuffer *evbuf, 
                                                struct tango_cache_meta_put *meta, 
                                                char *path/*OUT*/, size_t pathsize);

//流式上传接口
struct cache_evbase_ctx *object_store_update_start(struct object_store_instance *instance, struct future* f, struct tango_cache_meta_put *meta);
int object_store_update_frag_data(struct cache_evbase_ctx *ctx_asyn, enum PUT_MEMORY_COPY_WAY way, const char *data, size_t size);
int object_store_update_frag_evbuf(struct cache_evbase_ctx *ctx_asyn, struct evbuffer *evbuf);
int object_store_update_end(struct cache_evbase_ctx *ctx_asyn, char *path/*OUT*/, size_t pathsize);
void object_store_update_cancel(struct cache_evbase_ctx *ctx_asyn);

#endif


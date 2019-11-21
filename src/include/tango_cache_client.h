#ifndef __TANGO_CACHE_CLIENT_H__
#define __TANGO_CACHE_CLIENT_H__

#include <event2/event.h>  
#include <event.h>

#include <tfe_future.h>
#include "tango_cache_pending.h"

#define USER_TAG_MAX_LEN 1518

enum CACHE_ERR_CODE
{
	CACHE_OK=0,
	CACHE_CACHE_MISS  = -101, //缓存未命中
	CACHE_TIMEOUT     = -102,	//缓存超时
	CACHE_OUTOF_MEMORY= -103,//当前内存占用超过限制，查看MAX_USED_MEMORY_SIZE_MB是否过小或者当前上传速率跟不上调用者的速率
	CACHE_ERR_CURL    = -104,
	CACHE_ERR_WIREDLB = -105,
	CACHE_ERR_SOCKPAIR= -106,
	CACHE_ERR_INTERNAL= -107,
	CACHE_ERR_REDIS_JSON   = -108,
	CACHE_ERR_REDIS_CONNECT= -109,
	CACHE_ERR_REDIS_EXEC   = -110,
	CACHE_OUTOF_SESSION    = -111,
	CACHE_ERR_EVBUFFER     = -112,
	CACHE_UPDATE_CANCELED  = -113,
	CACHE_ERR_INSTANCE_ID  = -114,
	CACHE_ERR_BAD_REQUEST  = -115,
};

struct cache_statistics
{
	long long get_recv_num;  //发起GET的次数
	long long get_succ_http; //GET minio成功的次数
	long long get_succ_redis;//GET redis成功的次数
	long long get_miss_num;	 //GET未命中的次数
	long long get_err_http;  //GET minio失败的次数
	long long get_err_redis; //GET redis失败的次数
	long long put_recv_num;	 //发起UPLOAD的次数
	long long put_succ_http; //UPLOAD minio成功的次数
	long long put_succ_redis;//UPLOAD redis成功的次数
	long long put_err_http;  //UPLOAD minio失败的次数
	long long put_err_redis; //UPLOAD redis失败的次数
	long long put_cancel;    //UPLOAD Cancel次数
	long long put_losf_num;  //LOSF合并后的个数
	long long put_multipart; //开启了分段上传的对象个数
	long long put_duplicate; //LOSF重复数
	long long del_recv_num;	 //发起DELETE的次数
	long long del_succ_num;	 //DELETE成功的次数
	long long del_error_num; //DELETE失败的次数
	long long totaldrop_num; //内存满以及WiredLB出错时DROP的次数
	long long memory_used;	 //当前UPLOAD BODY所占内存大小
	long long session_http;	 //当前正在进行GET/PUT的HTTP会话数
	long long session_redis; //当前正在进行GET/PUT的redis会话数
};


struct tango_cache_parameter;
struct tango_cache_instance;
struct tango_cache_ctx;

enum CACHE_ERR_CODE tango_cache_get_last_error(const struct tango_cache_ctx *ctx);
enum CACHE_ERR_CODE tango_cache_ctx_error(const struct tango_cache_instance *instance);
long long tango_cache_maximum_session(const struct tango_cache_instance *instance);
void tango_cache_get_statistics(const struct tango_cache_instance *instance, struct cache_statistics *out);

/*每个进程执行一次初始化*/
void tango_cache_global_init(void);

//每个minio集群和bucket创建一个parameter，多个instance可共享一个parameter
struct tango_cache_parameter *tango_cache_parameter_new(const char* profile_path, const char* section, void *runtimelog);
/*以下所有API线程不安全*/
//每个监听线程创建一个instance
struct tango_cache_instance *tango_cache_instance_new(struct tango_cache_parameter *param, struct event_base* evbase, void *runtimelog);


/****************************************** GET接口的API ******************************************/
enum CACHE_RESULT_TYPE
{
	RESULT_TYPE_HEADER=0, //最多只调用一次
	RESULT_TYPE_USERTAG,  //最多只调用一次
	RESULT_TYPE_BODY,     //可能调用多次
	RESULT_TYPE_END,	  //全部结束，只调用一次，不附带数据
	RESULT_TYPE_MISS,	  //缓存未命中，与其他类型互斥，只调用一次(有END之意)，不附带数据
};

enum OBJECT_LOCATION
{
	OBJECT_IN_UNKNOWN=0,
	OBJECT_IN_MINIO,
	OBJECT_IN_REDIS
};

//Range支持3种: bytes=x-y; bytes=x-; bytes=-y
struct tango_cache_get_range
{
	int64_t start_offset; // -1表示不存在
	int64_t end_offset;   // -1表示不存在
};

struct tango_cache_meta_get
{
	const char* url; //缓存:URL；非结构化日志:文件路径名。CACHE_OBJECT_KEY_HASH_SWITCH=0时最大长度256字节，=1时无限制
	struct request_freshness get;
	struct tango_cache_get_range *range; //请求头Range
};

//promise_success的结果result
struct tango_cache_result
{
	const char *data_frag; //如果type为RESULT_TYPE_HEADER，每个头部后会包含一个换行(HTTP1.1格式)
	size_t size;   //本次片段的长度
	size_t tlength;//本次请求response body声称的总长度(无Range请求头时即对象的大小)，回调时都有效
	enum CACHE_RESULT_TYPE type;
	enum OBJECT_LOCATION location;
};

//成功时回调promise_success
//失败时回调promise_failed(仅一次)，使用get_last_error获取错误码；
//future不可以为NULL
int tango_cache_fetch_object(struct tango_cache_instance *instance, struct future* f, struct tango_cache_meta_get *meta, enum OBJECT_LOCATION where_to_get);
int tango_cache_head_object(struct tango_cache_instance *instance, struct future* f, struct tango_cache_meta_get *meta);
//从promise_success的result参数提取结果
struct tango_cache_result *tango_cache_read_result(future_result_t *promise_result);


/****************************************** DELETE接口的API ******************************************/
int tango_cache_delete_object(struct tango_cache_instance *instance, struct future* f, const char *objkey, const char *minio_addr=NULL, const char *bucket=NULL);


/****************************************** UPLOAD接口的API ******************************************/
/* 注意: 若future不为NULL，则在上传结束时会调用通知回调函数，否则不调用；*/

enum PUT_MEMORY_COPY_WAY
{
	PUT_MEM_COPY=0, //拷贝这块内存
	PUT_MEM_FREE,   //不拷贝内存，发送完毕由本缓存模块释放该内存
};
enum EVBUFFER_COPY_WAY
{
	EVBUFFER_MOVE=0,//evbuffer_add_buffer
	EVBUFFER_COPY,  //evbuffer_add_buffer_reference，不推荐
};

enum CACHE_HTTP_HDR_TYPE
{
	HDR_CONTENT_TYPE=0,
	HDR_CONTENT_ENCODING,
	HDR_CONTENT_DISPOSITION,
	HDR_CONTENT_MD5,
	
	HDR_CONTENT_NUM,
};

struct tango_cache_meta_put
{
	const char* url;
	const char* std_hdr[HDR_CONTENT_NUM]; //完整头部，如"Content-Type: text/html"，不要包含换行；NULL表示没有该头部；
	const char* usertag; //可以是任意内容，GET时会原样返回
	size_t usertag_len;  //最大长度USER_TAG_MAX_LEN，0表示没有该头部
	struct response_freshness put;
};

/****************************************** 完整一次UPLOAD接口的API ******************************************/
//若path不为空，则输出对象的存储路径
//返回值: 0-成功；<0失败；下同
int tango_cache_upload_once_data(struct tango_cache_instance *instance, struct future* f, 
                                            enum PUT_MEMORY_COPY_WAY way, const char *data, size_t size, 
                                            struct tango_cache_meta_put *meta, 
                                            char *path/*OUT*/, size_t pathsize);
int tango_cache_upload_once_evbuf(struct tango_cache_instance *instance, struct future* f, 
                                            enum EVBUFFER_COPY_WAY way, struct evbuffer *evbuf, 
                                            struct tango_cache_meta_put *meta, 
                                            char *path/*OUT*/, size_t pathsize);

/****************************************** 流式UPLOAD接口的API ******************************************/
//返回值: 若为NULL则表示创建失败，调用tango_cache_ctx_error查看错误码是否是CACHE_OUTOF_MEMORY(正常情况下是)；
struct tango_cache_ctx *tango_cache_update_start(struct tango_cache_instance *instance, struct future* f, struct tango_cache_meta_put *meta);
//返回值: 0-成功；<0失败，调用tango_cache_get_last_error查看错误码；
int tango_cache_update_frag_data(struct tango_cache_ctx *ctx, const char *data, size_t size);
int tango_cache_update_frag_evbuf(struct tango_cache_ctx *ctx, enum EVBUFFER_COPY_WAY way, struct evbuffer *evbuf);
//注意: 返回失败时不再调用promise回调函数，path不为NULL时返回存储路径
int tango_cache_update_end(struct tango_cache_ctx *ctx, char *path/*OUT*/, size_t pathsize);
//调用cancel后不会产生结果回调，认作传输失败
void tango_cache_update_cancel(struct tango_cache_ctx *ctx);

#endif


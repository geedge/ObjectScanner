#pragma once

#include<time.h>

enum tfe_http_std_field
{
    TFE_HTTP_UNKNOWN_FIELD = 0,
    TFE_HTTP_HOST,
    TFE_HTTP_REFERER,
    TFE_HTTP_USER_AGENT,
    TFE_HTTP_COOKIE,
    TFE_HTTP_PROXY_AUTHORIZATION,
    TFE_HTTP_AUTHORIZATION,
    TFE_HTTP_LOCATION,
    TFE_HTTP_SERVER,
    TFE_HTTP_ETAG,
    TFE_HTTP_DATE,
    TFE_HTTP_TRAILER,
    TFE_HTTP_TRANSFER_ENCODING,
    TFE_HTTP_VIA,
    TFE_HTTP_PRAGMA,
    TFE_HTTP_CONNECTION,
    TFE_HTTP_CONT_ENCODING,
    TFE_HTTP_CONT_LANGUAGE,
    TFE_HTTP_CONT_LOCATION,
    TFE_HTTP_CONT_RANGE,
    TFE_HTTP_CONT_LENGTH,
    TFE_HTTP_CONT_TYPE,
    TFE_HTTP_CONT_DISPOSITION,
    TFE_HTTP_EXPIRES,
    TFE_HTTP_ACCEPT_ENCODING,
    TFE_HTTP_CACHE_CONTROL,
    TLF_HTTP_IF_MATCH,
    TLF_HTTP_IF_NONE_MATCH,
    TLF_HTTP_IF_MODIFIED_SINCE,
    TLF_HTTP_IF_UNMODIFIED_SINCE,
    TLF_HTTP_LAST_MODIFIED
};


enum cache_pending_action {
    UNDEFINED = 0,
    ALLOWED,
    FORBIDDEN,
    VERIFY
};


struct tfe_http_field {

    enum tfe_http_std_field http_field;
    const char* value;
};


struct request_freshness {
    time_t min_fresh;
    time_t max_age;
};


struct response_freshness{
    time_t date;
    time_t last_modified;
    time_t timeout;
};

/*
函数功能：
根据请求头字段判断是否允许将缓存作为该请求的响应，并且将请求字段对缓存新鲜度的约束范围作为传出参数返回给调用者
参数:
request:HTTP请求字段信息，包括Pragma,Cache-Control,If-Match,If-None-Match,If-Modified-Since,If-Unmodified-Since字段
n_fields:request包含的HTTP请求字段数目
restrict:如果该函数返回值为ALLOWED，则返回请求Cache-Control字段包括的min-fresh或者max-age值；否则返回值为0
返回值：
UNDEFINED = 0,//请求字段中未定义缓存的行为
ALLOWED ,//允许使用缓存作为该请求的响应
FORBIDDEN,//禁止使用缓存作为该请求的响应，需要向源服务器请求
VERIFY,//禁止使用未验证有效性的缓存作为该请求的响应
*/
enum cache_pending_action tfe_cache_get_pending(const struct tfe_http_field *request, size_t n_fields,struct request_freshness* restrict);



/*
函数功能：
根据响应字段判断该响应是否允许被缓存，并且将响应的新鲜度、date或者last-modified值作为传出参数返回给调用者
参数:
response:HTTP响应字段信息，包括Pragma,Cache-Control,Expires、Date、Last-Modified字段
n_fields:response包含的HTTP响应字段数目
freshness:如果该函数返回值为ALLOWED，则返回响应Cache-Control字段的s-maxage/max-age值以及Date字段、Last-Modified字段相对时间
返回值：
UNDEFINED = 0,//响应字段中未定义缓存的行为
ALLOWED ,//允许缓存该响应
FORBIDDEN,//禁止缓存该响应
*/
enum cache_pending_action tfe_cache_put_pending(const struct tfe_http_field *response, size_t n_fields, struct response_freshness* freshness);
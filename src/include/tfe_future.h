#pragma once

enum e_future_error
{
	FUTURE_ERROR_CANCEL,
	FUTURE_ERROR_EXCEPTION,
	FUTURE_ERROR_TIMEOUT
};

struct promise;
struct future;
typedef void future_result_t;
typedef void (future_success_cb)(future_result_t* result, void * user);
typedef void (future_failed_cb)(enum e_future_error err, const char * what, void * user);
typedef void (promise_ctx_destroy_cb)(struct promise * p);

struct future * future_create(future_success_cb * cb_success, future_failed_cb * cb_failed, void * user);
struct future * promise_to_future(struct promise * p);
struct promise * future_to_promise(struct future * f);

void future_destroy(struct future * f);
void promise_failed(struct promise * p, enum e_future_error error, const char * what);
void promise_success(struct promise * p, void * result);
void promise_finish(struct promise * p);
void promise_allow_many_successes(struct promise *p);
void promise_set_ctx(struct promise * p, void * ctx, promise_ctx_destroy_cb * cb);
void * promise_get_ctx(struct promise * p);
void * promise_dettach_ctx(struct promise * p);

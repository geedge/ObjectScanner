#ifndef __OBJECT_SCANNER_KAFKA_H__
#define __OBJECT_SCANNER_KAFKA_H__

#include "cJSON.h"

#include "object_store_client.h"

enum MESSAGE_PROC_STATE
{
	MESSAGE_SUCC=0,
	MESSAGE_HITTED,
	MESSAGE_RCV_ERROR,
	MESSAGE_DROP,
	MESSAGE_NOT_EXIST,
	MESSAGE_ANLY_FAIL,
	MESSAGE_OTH_FAIL,

	MESSAGE_STATE_NUM,
};

struct objscan_statistics
{
	long num[MESSAGE_STATE_NUM];
};

typedef struct __message_meta_item
{
	cJSON *meta_json;
	char object_uri[256];
	time_t found_time;
	
	struct future * future;
	char *content;
	size_t current_len;
	size_t max_len;
	size_t object_size;
	int32_t hitted;
}message_meta_item_t;

int32_t rdkafka_consumer_init(void);
int32_t rdkafka_producer_init(void);

void destroy_parsed_message(message_meta_item_t *message);

void* thread_delay_msg_queue(void *arg);
void* thread_recv_kafka_msg(void *arg);

#endif


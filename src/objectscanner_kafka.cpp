#include <time.h>
#include <unistd.h> 
#include <stdio.h> 
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <sys/prctl.h>

#include "objectscanner_main.h"
#include "objectscanner_kafka.h"

#define OBJSCAN_RESULT_TOPIC "NTC-HTTP-OBJSCAN-RESULT"
#define OBJSCAN_HTTP_DOC_TOPIC "NTC-COLLECT-HTTP-DOC-LOG"
#define OBJSCAN_HTTP_EXE_TOPIC "NTC-COLLECT-HTTP-EXE-LOG"

extern objscan_global_info_t g_objscan_info;

static void kafka_conf_log_cb(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
	if(level <= g_objscan_info.sys_log_level)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "In log_cb, %s: %s", fac, buf);
	}
}

static void kafka_conf_error_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
	MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "In error_cb, error: %d, reason: %s", err, reason);
}

static void print_partition_list (const char *type, const rd_kafka_topic_partition_list_t *partitions) 
{
	int i;
	char buffer[2048];

	sprintf(buffer, "Consumer group rebalanced, type: %s, ", type);
	for (i = 0 ; i < partitions->cnt ; i++) 
	{
		snprintf(buffer+strlen(buffer), 2048-strlen(buffer), "%s %s[%u] offset %lu", 
			i > 0 ? ",":"", partitions->elems[i].topic, partitions->elems[i].partition, partitions->elems[i].offset);
	}
	MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "%s", buffer);
	printf("%s\n", buffer);
}

//设置一个不合法的OFFSET，以便从最新的数据开始消费
static void reset_kafka_offsets(rd_kafka_topic_partition_list_t *partitions)
{
	rd_kafka_resp_err_t err;
	int i;

	for (i = 0 ; i < partitions->cnt ; i++) 
	{
		err = rd_kafka_topic_partition_list_set_offset(partitions, partitions->elems[i].topic, partitions->elems[i].partition, -1);
		if(err != RD_KAFKA_RESP_ERR_NO_ERROR)
		{
			printf("Set Kafka offset error: %s\n", rd_kafka_err2str(err));
			MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "Set Kafka offset error: %s\n", rd_kafka_err2str(err));
		}
	}
}

static void kafka_rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque) 
{
	switch (err)
	{
		case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
			print_partition_list("assigned", partitions);
			if(g_objscan_info.consume_from_latest)
			{
				reset_kafka_offsets(partitions);
			}
			rd_kafka_assign(rk, partitions);
			break;

		case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
			print_partition_list("revoked", partitions);
			rd_kafka_assign(rk, NULL);
			break;

		default:
			MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "failed: %s\n", rd_kafka_err2str(err));
			rd_kafka_assign(rk, NULL);
			break;
	}
}

int32_t rdkafka_consumer_init(void)
{
	char errString[512] = {0}, confbuf[128];	
	rd_kafka_conf_t *config;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_topic_partition_list_t *topics;
	rd_kafka_resp_err_t err;
		
	/*Consumer configuration*/
	config = rd_kafka_conf_new();	
	rd_kafka_conf_set(config, "topic.metadata.refresh.interval.ms", "60000", errString, 512);
	if(rd_kafka_conf_set(config, "group.id", g_objscan_info.kafka_consu_name, errString, 512) != RD_KAFKA_CONF_OK)
	{
		printf("rd_kafka_conf_set group.id error: %s.\n", errString);
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "rd_kafka_conf_set group.id error: %s.\n", errString);
		return -1;
	}

	/*Topic configuration*/
	topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_topic_conf_set(topic_conf, "consume.callback.max.messages", confbuf, errString, 512);
	rd_kafka_topic_conf_set(topic_conf, "enable.auto.commit", "true", errString, 512);
	rd_kafka_topic_conf_set(topic_conf, "auto.offset.reset", "earliest", errString, 512); //RD_KAFKA_OFFSET_STORED初始没有offeset时
	if(rd_kafka_topic_conf_set(topic_conf, "offset.store.method", "broker", errString, 512) != RD_KAFKA_CONF_OK)
	{
		printf("rd_kafka_topic_conf_set offset.store.method(broker) error: %s.\n", errString);
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "rd_kafka_topic_conf_set offset.store.method(broker) error: %s.\n", errString);
		return -1;
	}
	rd_kafka_conf_set_default_topic_conf(config, topic_conf);
	rd_kafka_conf_set_rebalance_cb(config, kafka_rebalance_cb);
	rd_kafka_conf_set_log_cb(config, kafka_conf_log_cb);
	rd_kafka_conf_set_error_cb(config, kafka_conf_error_cb);
	
	if(!(g_objscan_info.kafka_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, config, errString, sizeof(errString))))
	{
		printf("rd_kafka_new error: %s.\n", errString);
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "rd_kafka_new error: %s.\n", errString);
		return -1;
	}

	if(rd_kafka_brokers_add(g_objscan_info.kafka_consumer, g_objscan_info.kafka_brokers) == 0)
	{
		printf("rd_kafka_brokers_add: %s error.\n", g_objscan_info.kafka_brokers);
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "rd_kafka_brokers_add %s error.", g_objscan_info.kafka_brokers);
		return -1;
	}
	rd_kafka_poll_set_consumer(g_objscan_info.kafka_consumer);

	topics = rd_kafka_topic_partition_list_new(2);
	rd_kafka_topic_partition_list_add(topics, OBJSCAN_HTTP_DOC_TOPIC, -1);
	rd_kafka_topic_partition_list_add(topics, OBJSCAN_HTTP_EXE_TOPIC, -1);
	err = rd_kafka_subscribe(g_objscan_info.kafka_consumer, topics);
	if(err)
	{
		printf("%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "Failed to start consuming topics: %s\n", rd_kafka_err2str(err));
		return -2;
	}
	return 0;
}

static void produce_delivery_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
	if(rkmessage->err)
	{
		atomic_inc(&(g_objscan_info.statistic.num[MESSAGE_OTH_FAIL]));
	}
	else
	{
		atomic_inc(&(g_objscan_info.statistic.num[MESSAGE_SUCC]));
	}
}

int32_t rdkafka_producer_init(void)
{
	char errString[512] = {0}, config_buf[128];	
	rd_kafka_conf_t *config;
	rd_kafka_topic_conf_t *topic_conf;

	/*Consumer configuration*/
	config = rd_kafka_conf_new();
	sprintf(config_buf, "%u", g_objscan_info.produce_q_size);
	rd_kafka_conf_set(config, "queue.buffering.max.messages", config_buf, errString, sizeof(errString));
	rd_kafka_conf_set(config, "topic.metadata.refresh.interval.ms", "60000", errString, sizeof(errString));
	rd_kafka_conf_set_dr_msg_cb(config, produce_delivery_msg_cb);

	if(!(g_objscan_info.kafka_producer = rd_kafka_new(RD_KAFKA_PRODUCER, config, errString, sizeof(errString))))
	{
		printf("rd_kafka_new error: %s.\n", errString);
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "rd_kafka_new error: %s.\n", errString);
		return -1;
	}
	if(rd_kafka_brokers_add(g_objscan_info.kafka_producer, g_objscan_info.kafka_brokers) == 0)
	{
		printf("rd_kafka_brokers_add: %s error.\n", g_objscan_info.kafka_brokers);
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL,  "rd_kafka_brokers_add %s error.", g_objscan_info.kafka_brokers);
		return -1;
	}

	/*Topic configuration*/
	sprintf(config_buf, "%u", g_objscan_info.kafka_req_ack);
	topic_conf = rd_kafka_topic_conf_new();
	rd_kafka_topic_conf_set(topic_conf, "request.required.acks", config_buf, errString, 512);
	g_objscan_info.produc_topic= rd_kafka_topic_new(g_objscan_info.kafka_producer, OBJSCAN_RESULT_TOPIC, topic_conf);
	if(NULL==g_objscan_info.produc_topic)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "create topic: %s error.", OBJSCAN_RESULT_TOPIC);
		return -1;
	}
	return 0;
}

void destroy_parsed_message(message_meta_item_t *message)
{
	future_destroy(message->future);
	free(message->content);

	cJSON_Delete(message->meta_json);
	free(message);
}

static message_meta_item_t *parse_rkmessage_cjson(const char* data, int datalen)
{
	cJSON *root, *psub = NULL;
	char *buffer = (char *)malloc(datalen+1), *pos_colon;
	message_meta_item_t *message;

	memcpy(buffer, data, datalen);
	buffer[datalen] = '\0';
	root = cJSON_Parse(buffer);
	free(buffer);
	if(NULL == root)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_INFO, "recv rkmessage error: cJSON_Parse failed.");		
		return NULL;
	}

	if(NULL==(psub=cJSON_GetObjectItem(root, "res_body_file")))
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_INFO, "recv rkmessage fail: path is NULL.");		
		cJSON_Delete(root); 
		return NULL;
	}

	if(psub->type!=cJSON_String || strncmp(psub->valuestring, "http://", strlen("http://")) || 
		NULL==(pos_colon=strchr(psub->valuestring+strlen("http://"), '/')))
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_INFO, "recv rkmessage fail: message format error.");		
		cJSON_Delete(root); 
		return NULL;
	}

	message = (message_meta_item_t *)calloc(1, sizeof(message_meta_item_t));
	message->meta_json = root;
	snprintf(message->object_uri, 256, "%s", pos_colon+1);
	
	if(NULL!=(psub=cJSON_GetObjectItem(root, "found_time")))
	{
		message->found_time = psub->valuedouble;
	}
	else
	{
		message->found_time = time(NULL);
	}
	return message;
}

void* thread_delay_msg_queue(void *arg)
{
	message_meta_item_t *message;
	long node_size;
	time_t now;
	
	prctl(PR_SET_NAME, "queue_delay");

	while(1)
	{
		node_size = sizeof(message_meta_item_t *);
		if(MESA_lqueue_get_head(g_objscan_info.queue_msg, (void *)&message, &node_size))
		{
			usleep(100000);
			continue;
		}
		now = time(NULL);
		if((message->found_time + g_objscan_info.delay_time_s) > now) //保证延迟g_objscan_info.delay_time_s时间
		{
			sleep(g_objscan_info.delay_time_s - (now - message->found_time));
		}
		
		if(MESA_lqueue_join_tail(g_objscan_info.queue_delay, (void *)&message, sizeof(message)) < 0)
		{
			atomic_inc(&(g_objscan_info.statistic.num[MESSAGE_DROP]));
			destroy_parsed_message(message);
		}
	}
	return NULL;
}

void* thread_recv_kafka_msg(void *arg)
{
	rd_kafka_message_t *rkmessage;
	message_meta_item_t *parsed_msg;
	int32_t ret;

	prctl(PR_SET_NAME, "kafka_rcvmsg");
	while(1)
	{
        rkmessage = rd_kafka_consumer_poll(g_objscan_info.kafka_consumer, g_objscan_info.consume_timeout_ms);
		if(rkmessage)
		{
			if(rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR)
			{
				if(NULL==(parsed_msg=parse_rkmessage_cjson((const char*)rkmessage->payload, rkmessage->len)))
				{
					atomic_inc(&(g_objscan_info.statistic.num[MESSAGE_RCV_ERROR]));
					rd_kafka_message_destroy(rkmessage);
					continue;
				}
				cJSON_AddStringToObject(parsed_msg->meta_json, "topic_name", rd_kafka_topic_name(rkmessage->rkt));

				ret = MESA_QUEUE_RET_CANT_GET_LOCK;
				while(ret==MESA_QUEUE_RET_CANT_GET_LOCK || ret==MESA_QUEUE_RET_GET_LOCK_TMOUT)
				{
					ret = MESA_lqueue_try_join_tail(g_objscan_info.queue_msg, (void *)&parsed_msg, sizeof(parsed_msg));
				}
				if(ret < 0)
				{
					atomic_inc(&(g_objscan_info.statistic.num[MESSAGE_DROP]));
					destroy_parsed_message(parsed_msg);
				}
			}
			rd_kafka_message_destroy(rkmessage);
		}
		else
		{
			usleep(200000);
		}
	}
	
	return NULL;
}


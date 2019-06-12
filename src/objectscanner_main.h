#ifndef __OBJECT_SCANNER_MAIN_H__
#define __OBJECT_SCANNER_MAIN_H__

#include <MESA/MESA_handle_logger.h>
#include <MESA/MESA_list_queue.h>
#include <MESA/field_stat2.h>

#include "librdkafka/rdkafka.h"

#include "objectscanner_kafka.h"

#define OBJSCAN_CONF_FILE "./conf/objectscan.conf"

#define MESA_HANDLE_RUNTIME_LOGV2(handle, lv, fmt, args...)   \
    MESA_handle_runtime_log((handle), (lv), "OBJSCAN", "%s:%d, " fmt, __FILE__, __LINE__, ##args)

enum FSSTAT_STATUS_ID
{
	FSSTAT_ID_MSG_QUEUE=0,
	FSSTAT_ID_DELAY_QUEUE,
	FSSTAT_ID_ANLYZ_QUEUE,
	FSSTAT_ID_kAFKA_P_Q,
	
	FSSTAT_ID_NUM,
};

typedef struct __colom_global_info
{
	//system
	MESA_lqueue_head queue_msg;
	MESA_lqueue_head queue_delay;
	MESA_lqueue_head queue_analyze;
	u_int32_t queue_elem_size;
	u_int32_t thread_num_anly;
	u_int32_t thread_num_fetch;
	u_int32_t delay_time_s;
	u_int32_t anly_max_len;

	struct object_store_instance *instance;

	//scan engine
	void *p_engine_handle;
	void *p_i2n_handle;
	
	//Kafka//
	u_int32_t kafka_req_ack;
	u_int32_t produce_q_size;
	u_int32_t consume_timeout_ms;
	u_int32_t consume_from_latest;
	char kafka_brokers[4096];
	char kafka_consu_name[128];
	rd_kafka_t *kafka_producer;
	rd_kafka_t *kafka_consumer;
	rd_kafka_topic_t *produc_topic;

	//LOGS
	char root_log_dir[256];
	char appname[64];
	u_int32_t log_level;
	int32_t sys_log_level;
	void *log_runtime;
	void *log_statistic;

	struct objscan_statistics statistic;
	screen_stat_handle_t fsstat_handle;
	char fsstat_dst_ip[64];
	char fsstat_appname[16];
	char fsstat_filepath[256];
	u_int32_t fsstat_period;
	u_int32_t fsstatid_trig;
	int32_t fsstat_dst_port;
	int32_t fsstat_field_ids[MESSAGE_STATE_NUM];
	int32_t fsstat_status_ids[FSSTAT_ID_NUM];
}objscan_global_info_t;

#if(__GNUC__ * 100 + __GNUC_MINOR__ * 10 + __GNUC_PATCHLEVEL__ >= 410)
#define atomic_inc(x) __sync_add_and_fetch((x),1)
#define atomic_dec(x) __sync_sub_and_fetch((x),1)
#define atomic_add(x,y) __sync_add_and_fetch((x),(y))
#define atomic_sub(x,y) __sync_sub_and_fetch((x),(y))
typedef long atomic_t;
#define ATOMIC_INIT(i)  { (i) }
#define atomic_read(x) __sync_add_and_fetch((x),0)
#define atomic_set(x,y) __sync_lock_test_and_set((x),y)
#else 
typedef long atomic_t;
#define atomic_inc(x)	((*(x))++)
#define atomic_dec(x)	((*(x))--)
#define atomic_add(x,y) ((*(x))+=(y))
#define atomic_sub(x,y) ((*(x))-=(y))
#define ATOMIC_INIT(i)  { (i) }
#define atomic_read(x) 	(*(x))
#define atomic_set(x,y)	((*(x))=(y))
#endif 

#endif


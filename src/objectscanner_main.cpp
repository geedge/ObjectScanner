#include <unistd.h> 
#include <stdio.h> 
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <sys/syslog.h>
#include <string.h>
#include <sys/stat.h>
#include <pthread.h>

#include <MESA/MESA_prof_load.h>
#include <MESA/wired_cfg.h>

#include "object_store_client.h"

#include "objectscanner_main.h"
#include "objectscanner_analyze.h"

objscan_global_info_t g_objscan_info;

static int mkdir_according_path(const char * path)
{
	char buffer[256];
	const char *ps=path, *pc;

	if(*ps == '/')
		ps += 1;
	
	while((pc = strchr(ps, '/')) != NULL)
	{
		while(*(pc+1) == '/')
			pc++;
			
		memcpy(buffer, path, pc - path);
		buffer[pc-path] = '\0';
		
		if(access(buffer, F_OK))
		{
			if(mkdir(buffer, 0777))
			{
				return -1;
			}
		}

		ps = pc + 1;
	}
	if(access(path, F_OK))
	{
		if(mkdir(path, 0777))
		{
			return -1;
		}
	}
	return 0;
}

void wired_load_config(const char *conf_path)
{
	void *wired_handle = wired_cfg_create("OBJECT_SCAN_CONF", conf_path);
	if(wired_cfg_init(wired_handle) != WCFG_RET_OK)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "wired_cfg_init %s failed.", conf_path);
	}
	wired_cfg_destroy(wired_handle);
}

static int _unfold_IP_range(char* ip_range, char***ip_list, int size)
{
	int i=0,count=0, ret=0;
	int range_digits[5];
	memset(range_digits,0,sizeof(range_digits));
	ret=sscanf(ip_range,"%d.%d.%d.%d-%d",&range_digits[0],&range_digits[1],&range_digits[2],&range_digits[3],&range_digits[4]);
	if(ret!=4&&ret!=5)
	{
		return 0;
	}
	if(ret==4&&range_digits[4]==0)
	{
		range_digits[4]=range_digits[3];
	}
	for(i=0;i<5;i++)
	{
		if(range_digits[i]<0||range_digits[i]>255)
		{
			return 0;
		}
	}
	count=range_digits[4]-range_digits[3]+1;
	*ip_list=(char**)realloc(*ip_list, sizeof(char*)*(size+count));
	for(i=0;i<count;i++)
	{
		(*ip_list)[size+i]=(char*)malloc(64);
		snprintf((*ip_list)[size+i],64,"%d.%d.%d.%d",range_digits[0],range_digits[1],range_digits[2],range_digits[3]+i);
	}
	return count;
}

static int unfold_IP_range(const char* ip_range, char***ip_list)
{
	char *token=NULL,*sub_token=NULL,*saveptr;
	char *buffer=(char*)calloc(sizeof(char),strlen(ip_range)+1);
	int count=0;
	strcpy(buffer,ip_range);
	for (token = buffer; ; token= NULL)
	{
		sub_token= strtok_r(token,";", &saveptr);
		if (sub_token == NULL)
			break;
		count+=_unfold_IP_range(sub_token, ip_list,count);	
	}
	free(buffer);
	return count;
}

static int build_kafka_cluster_addrs(const char *iplist, u_int32_t port, char *brokers, size_t size, void *runtimelog)
{
	u_int32_t redis_ip_num;
	char **redis_iplist=NULL;
	size_t addrlen;

	redis_ip_num = unfold_IP_range(iplist, &redis_iplist);
	if(redis_ip_num ==0 )
	{
		MESA_HANDLE_RUNTIME_LOGV2(runtimelog, RLOG_LV_FATAL, "decode KAFKA_BROKSER_IPLIST %s failed.", iplist);
		return -1;
	}

	memset(brokers, 0, size);
	for(u_int32_t i=0; i<redis_ip_num; i++)
	{
		addrlen = strlen(brokers);
		snprintf(brokers+addrlen, size-addrlen, "%s:%u,", redis_iplist[i], port);
		free(redis_iplist[i]);
	}
	addrlen = strlen(brokers);
	brokers[addrlen-1] = '\0';
	free(redis_iplist);
	return 0;
}

void register_field_stat(void)
{
	int value;
	const char *field_names[MESSAGE_STATE_NUM]={"SUCC", "HITTED", "RCV_ERROR", "DROP", "NOT_EXIST", "ANALYZE_FAIL", "OTH_FAIL"};
	const char *status_names[FSSTAT_ID_NUM]={"MSG_QUEUE", "DELAY_QUEUE", "ANALY_QUEUE", "KAFKA_P_Q"};
	
	g_objscan_info.fsstat_handle = FS_create_handle();
	FS_set_para(g_objscan_info.fsstat_handle, OUTPUT_DEVICE, g_objscan_info.fsstat_filepath, strlen(g_objscan_info.fsstat_filepath)+1);
	value = 1;
	FS_set_para(g_objscan_info.fsstat_handle, PRINT_MODE, &value, sizeof(value));
	value = 2;
	FS_set_para(g_objscan_info.fsstat_handle, STAT_CYCLE, &value, sizeof(value));
	value = 0;
	FS_set_para(g_objscan_info.fsstat_handle, CREATE_THREAD, &value, sizeof(value));
	FS_set_para(g_objscan_info.fsstat_handle, APP_NAME, g_objscan_info.fsstat_appname, strlen(g_objscan_info.fsstat_appname)+1);
	FS_set_para(g_objscan_info.fsstat_handle, STATS_SERVER_IP, g_objscan_info.fsstat_dst_ip, strlen(g_objscan_info.fsstat_dst_ip)+1);
	FS_set_para(g_objscan_info.fsstat_handle, STATS_SERVER_PORT, &g_objscan_info.fsstat_dst_port, sizeof(g_objscan_info.fsstat_dst_port));

	for(int i=0; i<MESSAGE_STATE_NUM; i++)
	{
		g_objscan_info.fsstat_field_ids[i] = FS_register(g_objscan_info.fsstat_handle, FS_STYLE_FIELD, FS_CALC_CURRENT, field_names[i]);
	}
	for(int i=0; i<FSSTAT_ID_NUM; i++)
	{
		g_objscan_info.fsstat_status_ids[i] = FS_register(g_objscan_info.fsstat_handle, FS_STYLE_STATUS, FS_CALC_CURRENT, status_names[i]);
	}
	FS_start(g_objscan_info.fsstat_handle);
}

static int32_t read_config_and_init(const char *config_file)
{
	char tmp_buf[4096], tmp_logdir[256];
	u_int32_t kafka_port;

	memset(&g_objscan_info, 0, sizeof(objscan_global_info_t));
	
	//log init//
	MESA_load_profile_int_def(config_file, "OBJSCAN", "SYS_LOG_LEVEL", &g_objscan_info.sys_log_level, LOG_INFO);
    MESA_load_profile_string_def(config_file, "OBJSCAN", "RUN_LOG_DIR", g_objscan_info.root_log_dir, sizeof(g_objscan_info.root_log_dir), "./log");
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "RUN_LOG_LV", &g_objscan_info.log_level, 10);
	sprintf(tmp_logdir, "%s/runtime_log", g_objscan_info.root_log_dir);
	if(mkdir_according_path(tmp_logdir))
	{
		printf("mkdir %s failed: %s\n", g_objscan_info.root_log_dir, strerror(errno));
		return -1;
	}
	snprintf(tmp_buf, 256, "%s/runtime.log", tmp_logdir);
    g_objscan_info.log_runtime = MESA_create_runtime_log_handle(tmp_buf, g_objscan_info.log_level);
    if(NULL==g_objscan_info.log_runtime)
    {
		printf("MESA_create_runtime_log_handle %s failed: %s\n", tmp_logdir, strerror(errno));
        return -1;
    }
	sprintf(tmp_logdir, "%s/log_statistic", g_objscan_info.root_log_dir);
	if(mkdir_according_path(tmp_logdir))
	{
		printf("mkdir %s failed: %s\n", g_objscan_info.root_log_dir, strerror(errno));
		return -1;
	}
	snprintf(tmp_buf, 256, "%s/statistic.log", tmp_logdir);
    g_objscan_info.log_statistic = MESA_create_runtime_log_handle(tmp_buf, g_objscan_info.log_level);
    if(NULL==g_objscan_info.log_statistic)
    {
		printf("MESA_create_runtime_log_handle %s failed: %s\n", tmp_logdir, strerror(errno));
        return -1;
    }
	
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "MAX_QUEUE_ELEMENTS", &g_objscan_info.queue_elem_size, 100000);
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "ANALYZE_MAX_SIZE", &g_objscan_info.anly_max_len, 1048576);
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "ANALYZE_THREAD_NUM", &g_objscan_info.thread_num_anly, 1);
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "FETCH_MINIO_THREAD_NUM", &g_objscan_info.thread_num_fetch, 1);
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "DELAY_ANALYZE_TIME_S", &g_objscan_info.delay_time_s, 20);

	MESA_load_profile_string_def(config_file, "OBJSCAN", "LOG_FSSTAT_APPNAME", g_objscan_info.fsstat_appname, 16, "ObjScan");
	MESA_load_profile_string_def(config_file, "OBJSCAN", "LOG_FSSTAT_FILEPATH", g_objscan_info.fsstat_filepath, 256, "./log/objscan_fsstat.log");
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "LOG_FSSTAT_INTERVAL", &g_objscan_info.fsstat_period, 60);
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "LOG_FSSTAT_TRIG", &g_objscan_info.fsstatid_trig, 1);
	MESA_load_profile_string_def(config_file, "OBJSCAN", "LOG_FSSTAT_DST_IP", g_objscan_info.fsstat_dst_ip, 64, "10.172.128.2");
	MESA_load_profile_int_def(config_file, "OBJSCAN", "LOG_FSSTAT_DST_PORT", &g_objscan_info.fsstat_dst_port, 8125);

	//KAFKA CONF//
	MESA_load_profile_string_def(config_file, "OBJSCAN", "KAFKA_CONSUMER_NAME", g_objscan_info.kafka_consu_name, 128, "ObjectScanner");
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "KAFKA_CONSUME_FROM_LATEST", &g_objscan_info.consume_from_latest, 0);
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "KAFKA_CONSUME_TIMEOUT_MS", &g_objscan_info.consume_timeout_ms, 5000);
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "KAFKA_PRODUCE_OUTQ_SIZE", &g_objscan_info.produce_q_size, 100000);
	MESA_load_profile_uint_def(config_file, "OBJSCAN", "KAFKA_REQUEST_ACKS", &g_objscan_info.kafka_req_ack, 0);
	if(MESA_load_profile_uint_def(config_file, "OBJSCAN", "KAFKA_BROKSER_PORT", &kafka_port, 9092)<0)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "Load config [OBJSCAN] KAFKA_BROKSER_PORT failed.");
		return -1;
	}
	if(MESA_load_profile_string_nodef(config_file, "OBJSCAN", "KAFKA_BROKSER_IPLIST", tmp_buf, sizeof(tmp_buf))<0)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "Load config [OBJSCAN] KAFKA_BROKSER_IPLIST failed.");
		return -1;
	}

	g_objscan_info.queue_msg     = MESA_lqueue_create(1, g_objscan_info.queue_elem_size);
	g_objscan_info.queue_delay   = MESA_lqueue_create(1, g_objscan_info.queue_elem_size);
	g_objscan_info.queue_analyze = MESA_lqueue_create(1, g_objscan_info.queue_elem_size);

	register_field_stat();
	return build_kafka_cluster_addrs(tmp_buf, kafka_port, g_objscan_info.kafka_brokers, 4096, g_objscan_info.log_runtime);
}

int32_t main(int32_t argc, char **argv)
{
    pthread_t thread_desc;
	pthread_attr_t attr;
	u_int32_t i;

	wired_load_config(OBJSCAN_CONF_FILE);
	if(read_config_and_init(OBJSCAN_CONF_FILE) || rdkafka_producer_init() || rdkafka_consumer_init() || avl_scan_engine_init())
	{
		assert(0);
		return -1;
	}

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	object_store_global_init();
	g_objscan_info.instance = object_store_instance_new(OBJSCAN_CONF_FILE, "TANGO_CACHE", g_objscan_info.thread_num_fetch, g_objscan_info.log_runtime);
	if(g_objscan_info.instance == NULL)
	{
		assert(0);
		return -1;
	}

	if(pthread_create(&thread_desc, &attr, thread_recv_kafka_msg, NULL))
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "pthread_create(): %s", strerror(errno));
		return -1;
	}
	if(pthread_create(&thread_desc, &attr, thread_delay_msg_queue, NULL))
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "pthread_create(): %s", strerror(errno));
		return -1;
	}
	if(pthread_create(&thread_desc, &attr, thread_fetch_object, NULL))
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "pthread_create(): %s", strerror(errno));
		return -1;
	}
	
	for(i=0; i<g_objscan_info.thread_num_anly; i++)
	{
		if(pthread_create(&thread_desc, &attr, thread_analyze_object, NULL))
		{
			MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "pthread_create(): %s", strerror(errno));
			return -1;
		}
	}

	/*statistics*/
	time_t now, remain;
	long queue_msg, queue_delay, queue_anly, quque_kafka;
	struct objscan_statistics last_statistic, inc_statistic;
	long *pinc_statistic=(long*)&inc_statistic, *pnow_statistic=(long*)&g_objscan_info.statistic, *plast_statistic=(long*)&last_statistic;

	memset(&last_statistic, 0, sizeof(struct objscan_statistics));
	while(1)
	{
		now = time(NULL);
		remain = g_objscan_info.fsstat_period- (now % g_objscan_info.fsstat_period);
		sleep(remain);

		rd_kafka_poll(g_objscan_info.kafka_producer, 0); //驱动更新Kafka的统计信息
		for(i=0; i<sizeof(struct objscan_statistics)/sizeof(long); i++)
		{
			pinc_statistic[i] = pnow_statistic[i] - plast_statistic[i];
		}
		last_statistic = g_objscan_info.statistic;

		for(i=0; i<MESSAGE_STATE_NUM; i++)
		{
			FS_operate(g_objscan_info.fsstat_handle, g_objscan_info.fsstat_field_ids[i], 0, FS_OP_SET, g_objscan_info.statistic.num[i]);
		}
		queue_msg = MESA_lqueue_get_count(g_objscan_info.queue_msg);
		FS_operate(g_objscan_info.fsstat_handle, g_objscan_info.fsstat_status_ids[FSSTAT_ID_MSG_QUEUE], 0, FS_OP_SET, queue_msg);
		queue_delay = MESA_lqueue_get_count(g_objscan_info.queue_delay);
		FS_operate(g_objscan_info.fsstat_handle, g_objscan_info.fsstat_status_ids[FSSTAT_ID_DELAY_QUEUE], 0, FS_OP_SET, queue_delay);
		queue_anly = MESA_lqueue_get_count(g_objscan_info.queue_analyze);
		FS_operate(g_objscan_info.fsstat_handle, g_objscan_info.fsstat_status_ids[FSSTAT_ID_ANLYZ_QUEUE], 0, FS_OP_SET, queue_anly);
		quque_kafka = rd_kafka_outq_len(g_objscan_info.kafka_producer);
		FS_operate(g_objscan_info.fsstat_handle, g_objscan_info.fsstat_status_ids[FSSTAT_ID_kAFKA_P_Q], 0, FS_OP_SET, quque_kafka);
		FS_passive_output(g_objscan_info.fsstat_handle);

		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_statistic, RLOG_LV_FATAL, "----------------------------------------------------------------------------------");
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_statistic, RLOG_LV_FATAL, "SCAN_SUCC: %lu, HITTED: %lu, RCV_ERROR: %lu, DROP: %lu, EMPTY: %lu, ANLY_FAIL: %lu, OTH_FAIL: %lu", 
			pinc_statistic[MESSAGE_SUCC], pinc_statistic[MESSAGE_HITTED], pinc_statistic[MESSAGE_RCV_ERROR], pinc_statistic[MESSAGE_DROP],
			pinc_statistic[MESSAGE_NOT_EXIST], pinc_statistic[MESSAGE_ANLY_FAIL], pinc_statistic[MESSAGE_OTH_FAIL]);
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_statistic, RLOG_LV_FATAL, "MSG_QUEUE: %lu, DELAY_QUEUE: %lu, ANLY_QUEUE: %lu, KAFKA_P_Q: %lu", 
			queue_msg, queue_delay, queue_anly, quque_kafka);
	}
	return 0;
}


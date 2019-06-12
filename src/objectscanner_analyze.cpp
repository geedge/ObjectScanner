#include <unistd.h> 
#include <stdio.h> 
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <sys/prctl.h>
#include <string.h>

#include "librdkafka/rdkafka.h"

#include "avlsdk/engine.h"
#include "avlsdk/error_code.h"
#include "avlsdk/AVLSDK_rpt_idx.h"
#include "avlsdk/AVLSDK_conf_idx.h"
#include "avlsdk/ID2Name_interface.h"

#include "object_store_client.h"

#include "objectscanner_main.h"
#include "objectscanner_kafka.h"
#include "objectscanner_analyze.h"

extern objscan_global_info_t g_objscan_info;

int avl_scan_engine_init(void)
{
	long ret, long_last_ret;
	void *p_engine_handle=NULL, *p_i2n_handle=NULL;
	void							*p_sdk_handle = NULL, *p_name_handle = NULL;

	ret = AVL_SDK_CreateInstance(&p_engine_handle);
	if(ret != ERR_SUCCESS)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_SDK_CreateInstance error: %ld", ret);
		return -1;
	}
	ret = AVL_SDK_LoadConfigFile(p_engine_handle, (char *)DEFAULT_CONFIG_FILE);
	if(ret != ERR_SUCCESS)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_SDK_LoadConfigFile error: %ld", ret);
		return -1;
	}
	ret = AVL_SDK_SetConfigInt(p_engine_handle, CFG_INT_APACK_RECURE_LAYER, 5);
	if(ret != ERR_SUCCESS)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_SDK_SetConfigInt error: %ld", ret);
		return -1;
	}
	ret = AVL_SDK_SetConfigString(p_engine_handle, CFG_STR_LICENSE_PATH, DEFAULT_LICENCE_FILE);
	if(ret != ERR_SUCCESS)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_SDK_SetConfigString error: %ld", ret);
		return -1;
	}
	ret = AVL_SDK_InitInstance(p_engine_handle, NULL);
	if(ret != ERR_SUCCESS)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_SDK_InitInstance error: %ld", ret);
		return -1;
	}

	ret = AVL_NTranser_Init(DEFAULT_ID2N_DB_PATH, &p_i2n_handle);
	if(ret != ERR_SUCCESS)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_NTranser_Init error: %ld", ret);
		return -1;
	}

	g_objscan_info.p_engine_handle = p_engine_handle;
	g_objscan_info.p_i2n_handle    = p_i2n_handle;
	return 0;
}

static long	func_long_query_continue_callback(void *p_param)
{
	message_meta_item_t *message = (message_meta_item_t *)p_param;

	// This is the code sample, so it returns unconditionally. Users can modify according to the condition.
	//有很多子文件的情况，一个子文件扫中后不再继续扫了
	return	(message->hitted)?OD_ABORT:OD_CONTINUE;
}

static long	func_long_get_rslt_callback(P_OBJ_PROVIDER p_op, void *p_data, void *p_param)
{
	long long_malware_id=0, long_qry_ret=-1;
	message_meta_item_t *message = (message_meta_item_t *)p_param;
	unsigned char *puchar_analyser=NULL, auchar_malware_name[128]={0};
	char *malware_type, *malware_name;
	char *save_ptr;
	
	if(p_data == NULL)
	{
		return -1;
	}
	// Query the Malware ID
	long_qry_ret = AVL_SDK_QueryReportInt(g_objscan_info.p_engine_handle, p_data, RPT_IDX_MALWARE_ID, &long_malware_id);
	if (long_qry_ret==ERR_RPT_NOT_EXIST || long_qry_ret<0)
	{
		return long_qry_ret;
	}

	// Query the analyser who detected this malware, users will use it when they need to get the malware name
	long_qry_ret = AVL_SDK_QueryReportStr(g_objscan_info.p_engine_handle, p_data, RPT_IDX_ANALYSER, &puchar_analyser);
	if (long_qry_ret != ERR_SUCCESS)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_SDK_QueryReportStr error: %ld", long_qry_ret);
		return long_qry_ret;
	}

	// Query the VName
	long_qry_ret = AVL_NTranser_QueryNameByID(g_objscan_info.p_i2n_handle, (char *)puchar_analyser, long_malware_id, (unsigned char *)auchar_malware_name, sizeof(auchar_malware_name) - 1);
	if (long_qry_ret < 0)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_NTranser_QueryNameByID error: %ld", long_qry_ret);
		return long_qry_ret;
	}

	/* AVL命中后, auchar_malware_name格式大概是"Trojan/Win32.SGeneric", 需要拆分 */
	malware_type = strtok_r((char *)auchar_malware_name, "/", &save_ptr);
	malware_name = strtok_r(NULL, "/", &save_ptr);
	if(NULL == malware_name)
	{
		malware_name = malware_type;
	}
	cJSON_AddNumberToObject(message->meta_json, "malware_id", long_malware_id);
	cJSON_AddStringToObject(message->meta_json, "malware_type", malware_type);
	cJSON_AddStringToObject(message->meta_json, "malware_name", malware_name);
	message->hitted = 1;
	MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_DEBUG, "AVL_SDK_Scan %s hit, type: %s, name: %s", message->object_uri, malware_type, malware_name);
	atomic_inc(&g_objscan_info.statistic.num[MESSAGE_HITTED]);
	return long_qry_ret;
}

static int32_t scan_object_by_avl_engine(message_meta_item_t *message)
{
	long long_last_ret = 0;
	OBJ_PROVIDER op = {0};
	OBJ_DISPOSER od = {0};

	op.obj_ver = CUR_ENGINE_VER;
	op.evro_type = ET_DESKTOP;
	op.buf = (unsigned char *)message->content;
	op.size = message->current_len;
	strncpy((char *)op.obj_des, message->object_uri, strlen(message->object_uri));

	od.rpt_callback = func_long_get_rslt_callback;
	od.p_rpt_param = message;
	od.query_continue_callback = func_long_query_continue_callback;
	od.p_qc_param = message;

	long_last_ret = AVL_SDK_Scan(g_objscan_info.p_engine_handle, &op, &od);
	if (long_last_ret < 0)
	{
		MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_FATAL, "AVL_SDK_Scan error: %ld", long_last_ret);
		return 0;
	}
	return message->hitted;
}

void* thread_analyze_object(void *arg)
{
	message_meta_item_t *message;
	char *buffer;
	size_t buflen;
	long node_size;
	int ret=0;

	prctl(PR_SET_NAME, "anly_object");
	while(1)
	{
		node_size = sizeof(message_meta_item_t *);
		if(MESA_lqueue_get_head(g_objscan_info.queue_analyze, (void *)&message, &node_size))
		{
			usleep(100000);
			continue;
		}

		if(scan_object_by_avl_engine(message))
		{
			buffer = cJSON_PrintUnformatted(message->meta_json);
			buflen = strlen(buffer);
			ret = rd_kafka_produce(g_objscan_info.produc_topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE, buffer, buflen, NULL, 0, NULL);
			rd_kafka_poll(g_objscan_info.kafka_producer, 0);
			if(ret!=0)
			{
				MESA_HANDLE_RUNTIME_LOGV2(g_objscan_info.log_runtime, RLOG_LV_DEBUG, "rd_kafka_produce error: %d", rd_kafka_last_error());
				atomic_inc(&g_objscan_info.statistic.num[MESSAGE_OTH_FAIL]);
				free(buffer);
			}
		}
		else
		{
			atomic_inc(&g_objscan_info.statistic.num[MESSAGE_SUCC]);
		}
		destroy_parsed_message(message);
	}
	return NULL;
}

void get_future_failed(enum e_future_error err, const char * what, void * user)
{
	message_meta_item_t *message = (message_meta_item_t *)user;
	
	atomic_inc(&(g_objscan_info.statistic.num[MESSAGE_OTH_FAIL]));
	destroy_parsed_message(message);
}

void get_future_success(future_result_t* result, void * user)
{
	struct tango_cache_result *res = cache_evbase_read_result(result);
	message_meta_item_t *message = (message_meta_item_t *)user;

	switch(res->type)
	{
		case RESULT_TYPE_USERTAG:
		case RESULT_TYPE_HEADER:
			if(message->content == NULL)
			{
				message->max_len = (res->tlength>=g_objscan_info.anly_max_len)?g_objscan_info.anly_max_len:res->tlength;
				message->content = (char *)malloc(message->max_len);
			}
			break;

		case RESULT_TYPE_BODY:
			if(message->current_len+res->size <= message->max_len)
			{
				memcpy(message->content+message->current_len, res->data_frag, res->size);
				message->current_len += res->size;
			}
			break;
			
		case RESULT_TYPE_MISS:
			atomic_inc(&(g_objscan_info.statistic.num[MESSAGE_NOT_EXIST]));
			destroy_parsed_message(message);
			break;
			
		case RESULT_TYPE_END:
			if(MESA_lqueue_join_tail(g_objscan_info.queue_analyze, (void *)&message, sizeof(message)) < 0)
			{
				atomic_inc(&g_objscan_info.statistic.num[MESSAGE_DROP]);
				destroy_parsed_message(message);
			}
			break;
		default:break;
	}
}

void* thread_fetch_object(void *arg)
{
	message_meta_item_t *message;
	long node_size;
	struct tango_cache_meta_get getmeta;

	prctl(PR_SET_NAME, "fetch_object");
	memset(&getmeta, 0, sizeof(struct tango_cache_meta_get));
	while(1)
	{
		node_size = sizeof(message_meta_item_t *);
		if(MESA_lqueue_get_head(g_objscan_info.queue_delay, (void *)&message, &node_size))
		{
			usleep(100000);
			continue;
		}
		
		message->future = future_create(get_future_success, get_future_failed, message);

		getmeta.url = message->object_uri;
		if(object_store_fetch_object(g_objscan_info.instance, message->future, &getmeta, OBJECT_IN_UNKNOWN) < 0)
		{
			get_future_failed(FUTURE_ERROR_CANCEL, "", message);
		}
	}
	return NULL;
}


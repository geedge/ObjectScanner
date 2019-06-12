#ifndef __ASMIS_LOG_H
#define __ASMIS_LOG_H
#ifndef __cplusplus
#error("This file should be compiled with C++ compiler")
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


//#include <support/thread_safe.h>

#define ASMIS_KEY 		0x01
#define ASMIS_ALARM 	0x02
#define ASMIS_OTHER 	0x03

#define ASMIS_LOGMSG_TIMEOUT	300	//second
#define ASMIS_LOGMSG_LENGTH	40000	//bytes

//系统使用端口信息结构体
struct info_port_used {
	unsigned short nPort;
	unsigned char nProtocolType;
	unsigned char nPortType;
	char sPortDesc[128];
};

//系统实时流量信息结构体
struct info_rtd_flow {
	char sValType[32];
	char sBDType[32];
	time_t nRTTS;
	int nDuration;
	unsigned long long nValue;
};

//系统策略更新信息结构体
struct info_policy_update {
	char sName[128];
	char sDesc[256];
	time_t nUpdateTime;
	char sVersion[33];
	int nTotal;
	int nNew;
	int nDelete;
	int nUpdate;
	int nSize;
};

//NetLog初始化
void* asmis_log_Init(const char *pProcName);
//登记程序版本信息
int asmis_log_AppVer(void* netlog_handle,const char *pVersionTime, const char *pVersionNO, const char *pVersionDesc);
//系统运行日志
int asmis_log_LogMsg(void* netlog_handle,const char *pMsg, const char *pNo, int nAlarmType);
//登记系统使用端口
int asmis_log_PortUsed(void* netlog_handle,struct info_port_used *info, int nPort);
//登记系统实时流量信息
int asmis_log_RtdFlow(void* netlog_handle,time_t nStartTime, int nDuration, struct info_rtd_flow *info, int nFlow);
//登记系统开始运行
int asmis_log_RunStart(void* netlog_handle,int nContiRun);
//登记系统停止信息
int asmis_log_RunStop(void* netlog_handle,int nContiRun);
//心跳信息
int asmis_log_HeartBeat(void* netlog_handle,const char *pMsg);
//策略更新信息
int asmis_log_Policy(void* netlog_handle,struct info_policy_update *info, int nPolicy);

#endif

#ifndef __OBJECT_SCANNER_ANALYZE_H__
#define __OBJECT_SCANNER_ANALYZE_H__

#define DEFAULT_SDK_PATH     "./avlsdk3/AVLSDK.so"
#define DEFAULT_ID2NAME_PATH "./avlsdk3/aid2name.so"
#define	DEFAULT_ID2N_DB_PATH "./avlsdk3/NData"
#define	DEFAULT_CONFIG_FILE  "./avlsdk3/Config/high_scan.ct"
#define DEFAULT_LICENCE_FILE "./avlsdk3/License.alf"

int avl_scan_engine_init(void);

void* thread_analyze_object(void *arg);
void* thread_fetch_object(void *arg);

#endif


CCC=g++
CC=gcc

INC_PATH=-I./include 
CFLAGS= -g $(INC_PATH)
#CFLAGS=-Wall -g $(INC_PATH)
LDFLAGS = 
LIBS = ./libs/libtango_cache_client.a ./libs/libcjson.a  ./libs/libcrypto.a  ./libs/libcurl.a  ./libs/libevent.a  ./libs/libhiredis_vip.a  ./libs/libssl.a  ./libs/libxml2.a
LIBS += -lMESA_handle_logger -lMESA_htable -lMESA_prof_load -lMESA_field_stat2 -lpthread -lwiredcfg -lWiredLB -lrdkafka
LIBS += -laid2name -lAVLSDK

OBJS = objectscanner_analyze.o objectscanner_kafka.o objectscanner_main.o
DEPS = $(OBJS:.o=.d)

TARGET_EXE=object_scanner

ALL:$(TARGET_EXE)

$(TARGET_EXE):$(OBJS)
	$(CCC) $(LDFLAGS) $^ -o $@ $(LIBS)
	cp $@ ../bin/

.c.o:
	$(CCC) $(CFLAGS) -c $<

.cpp.o:
	$(CCC) $(CFLAGS) -c $<

-include $(DEPS)

clean:
	rm -rf $(OBJS) $(DEPS) $(TARGET_EXE)


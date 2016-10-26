CC = gcc
CFLAGS = -g -Wall --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -I. -I../common -I../common/thirdparty
BUILD = build

all: $(BUILD)/plasma_store $(BUILD)/plasma_manager $(BUILD)/plasma_client.so $(BUILD)/example $(BUILD)/libplasma_client.a

debug: FORCE
debug: CFLAGS += -DRAY_COMMON_DEBUG=1
debug: all

clean:
	cd ../common; make clean
	rm -r $(BUILD)/*

$(BUILD)/plasma_store: plasma_store.c plasma.h fling.h fling.c malloc.c malloc.h thirdparty/dlmalloc.c common
	$(CC) $(CFLAGS) plasma_store.c fling.c malloc.c ../common/build/libcommon.a -o $(BUILD)/plasma_store

$(BUILD)/plasma_manager: plasma_manager.c plasma.h plasma_client.c fling.h fling.c common
	$(CC) $(CFLAGS) plasma_manager.c plasma_client.c fling.c ../common/build/libcommon.a ../common/thirdparty/hiredis/libhiredis.a -o $(BUILD)/plasma_manager

$(BUILD)/plasma_client.so: plasma_client.c fling.h fling.c common
	$(CC) $(CFLAGS) plasma_client.c fling.c ../common/build/libcommon.a -fPIC -shared -o $(BUILD)/plasma_client.so

$(BUILD)/libplasma_client.a: plasma_client.o fling.o
	ar rcs $@ $^

$(BUILD)/example: plasma_client.c plasma.h example.c fling.h fling.c common
	$(CC) $(CFLAGS) plasma_client.c example.c fling.c ../common/build/libcommon.a -o $(BUILD)/example

common: FORCE
		cd ../common; make

# Set the request timeout low for testing purposes.
test: CFLAGS += -DRAY_TIMEOUT=50
test: FORCE
		cd ../common; make redis
test: all

FORCE:

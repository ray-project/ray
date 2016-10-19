CC = gcc
CFLAGS = -g -Wall --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -I. -Icommon -Icommon/thirdparty
BUILD = build

all: $(BUILD)/plasma_store $(BUILD)/plasma_manager $(BUILD)/plasma_client.so $(BUILD)/example $(BUILD)/libplasma_client.a

debug: FORCE
debug: CFLAGS += -DRAY_COMMON_DEBUG=1
debug: all

clean:
	cd common; make clean
	rm -r $(BUILD)/*

$(BUILD)/plasma_store: src/plasma_store.c src/plasma.h src/fling.h src/fling.c src/malloc.c src/malloc.h thirdparty/dlmalloc.c common
	$(CC) $(CFLAGS) src/plasma_store.c src/fling.c src/malloc.c common/build/libcommon.a -o $(BUILD)/plasma_store

$(BUILD)/plasma_manager: src/plasma_manager.c src/plasma.h src/plasma_client.c src/fling.h src/fling.c common
	$(CC) $(CFLAGS) src/plasma_manager.c src/plasma_client.c src/fling.c common/build/libcommon.a common/thirdparty/hiredis/libhiredis.a -o $(BUILD)/plasma_manager

$(BUILD)/plasma_client.so: src/plasma_client.c src/fling.h src/fling.c common
	$(CC) $(CFLAGS) src/plasma_client.c src/fling.c common/build/libcommon.a -fPIC -shared -o $(BUILD)/plasma_client.so

$(BUILD)/libplasma_client.a: src/plasma_client.o src/fling.o
	ar rcs $@ $^

$(BUILD)/example: src/plasma_client.c src/plasma.h src/example.c src/fling.h src/fling.c common
	$(CC) $(CFLAGS) src/plasma_client.c src/example.c src/fling.c common/build/libcommon.a -o $(BUILD)/example

common: FORCE
		git submodule update --init --recursive
		cd common; make

# Set the request timeout low for testing purposes.
test: CFLAGS += -DRAY_TIMEOUT=50
test: FORCE
		cd common; make redis
test: all

FORCE:

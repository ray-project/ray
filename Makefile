CC = gcc
CFLAGS = -g -Wall --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -I.
BUILD = build

all: $(BUILD)/plasma_store $(BUILD)/plasma_manager $(BUILD)/plasma_client.so $(BUILD)/example

clean:
	rm -r $(BUILD)/*

$(BUILD)/plasma_store: src/plasma_store.c src/plasma.h src/event_loop.h src/event_loop.c src/fling.h src/fling.c src/malloc.c src/malloc.h thirdparty/dlmalloc.c
	$(CC) $(CFLAGS) src/plasma_store.c src/event_loop.c src/fling.c src/malloc.c -o $(BUILD)/plasma_store

$(BUILD)/plasma_manager: src/plasma_manager.c src/event_loop.h src/event_loop.c src/plasma.h src/plasma_client.c src/fling.h src/fling.c
	$(CC) $(CFLAGS) src/plasma_manager.c src/event_loop.c src/plasma_client.c src/fling.c -o $(BUILD)/plasma_manager

$(BUILD)/plasma_client.so: src/plasma_client.c src/fling.h src/fling.c
	$(CC) $(CFLAGS) src/plasma_client.c src/fling.c -fPIC -shared -o $(BUILD)/plasma_client.so

$(BUILD)/example: src/plasma_client.c src/plasma.h src/example.c src/fling.h src/fling.c
	$(CC) $(CFLAGS) src/plasma_client.c src/example.c src/fling.c -o $(BUILD)/example

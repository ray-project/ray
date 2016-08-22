CC = gcc
CFLAGS = -g -Wall
BUILD = build

all: $(BUILD)/plasma_store $(BUILD)/plasma_manager $(BUILD)/plasma_client.so $(BUILD)/example

clean:
	rm -r $(BUILD)/*

$(BUILD)/plasma_store: src/plasma_store.c src/plasma.h src/fling.h src/fling.c
	$(CC) $(CFLAGS) --std=c99 -D_XOPEN_SOURCE=500 src/plasma_store.c src/fling.c -o $(BUILD)/plasma_store

$(BUILD)/plasma_manager: src/plasma_manager.c src/plasma.h src/plasma_client.c src/fling.h src/fling.c
	$(CC) $(CFLAGS) --std=c99 -D_XOPEN_SOURCE=500 src/plasma_manager.c src/plasma_client.c src/fling.c -o $(BUILD)/plasma_manager

$(BUILD)/plasma_client.so: src/plasma_client.c src/fling.h src/fling.c
	$(CC) $(CFLAGS) --std=c99 -D_XOPEN_SOURCE=500 src/plasma_client.c src/fling.c -fPIC -shared -o $(BUILD)/plasma_client.so

$(BUILD)/example: src/plasma_client.c src/plasma.h src/example.c src/fling.h src/fling.c
	$(CC) $(CFLAGS) --std=c99 -D_XOPEN_SOURCE=500 src/plasma_client.c src/example.c src/fling.c -o $(BUILD)/example

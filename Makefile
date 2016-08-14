CC = gcc
CFLAGS = -g -Wall
BUILD = build

all: $(BUILD)/plasma_store $(BUILD)/example

clean:
	rm $(BUILD)/*

$(BUILD)/plasma_store: src/plasma_store.c src/plasma.h src/fling.h src/fling.c
	$(CC) $(CFLAGS) --std=c99 -D_XOPEN_SOURCE=500 src/plasma_store.c src/fling.c -o $(BUILD)/plasma_store

$(BUILD)/example: src/plasma_client.c src/plasma.h src/example.c src/fling.h src/fling.c
	$(CC) $(CFLAGS) --std=c99 -D_XOPEN_SOURCE=500 src/plasma_client.c src/example.c src/fling.c -o $(BUILD)/example

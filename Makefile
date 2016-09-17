CC = gcc
CFLAGS = -g -Wall --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L
BUILD = build

CFLAGS += -Wmissing-prototypes
CFLAGS += -Wstrict-prototypes
CFLAGS += -Wmissing-declarations

$(BUILD)/db_tests: hiredis test/db_tests.c thirdparty/greatest.h event_loop.c state/redis.c common.c
	$(CC) -o $@ test/db_tests.c event_loop.c state/redis.c common.c thirdparty/hiredis/libhiredis.a $(CFLAGS) -I. -Ithirdparty

$(BUILD)/socket_tests: test/socket_tests.c thirdparty/greatest.h sockets.c
	$(CC) -o $@ test/socket_tests.c sockets.c $(CFLAGS) -I. -Ithirdparty

$(BUILD)/task_tests: test/task_tests.c task.c sockets.c common.h
	$(CC) -o $@ test/task_tests.c task.c sockets.c $(CFLAGS) -I. -Ithirdparty

clean:
	rm -r $(BUILD)/*

redis:
	cd thirdparty ; bash ./build-redis.sh

hiredis:
	git submodule update --init --recursive -- "thirdparty/hiredis" ; cd thirdparty/hiredis ; make

test: hiredis redis $(BUILD)/db_tests $(BUILD)/socket_tests $(BUILD)/task_tests FORCE
	./thirdparty/redis-3.2.3/src/redis-server &
	sleep 1s ; ./build/db_tests ; ./build/socket_tests ; ./build/task_tests

FORCE:

CC = gcc
CFLAGS = -g -Wall --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -fPIC -I. -Ithirdparty -Ithirdparty/ae -Wno-typedef-redefinition -Werror
BUILD = build

all: hiredis $(BUILD)/libcommon.a

$(BUILD)/libcommon.a: event_loop.o common.o task.o io.o state/redis.o thirdparty/ae/ae.o
	ar rcs $@ $^

$(BUILD)/common_tests: test/common_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ test/common_tests.c $(BUILD)/libcommon.a $(CFLAGS)

$(BUILD)/db_tests: hiredis test/db_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ test/db_tests.c $(BUILD)/libcommon.a thirdparty/hiredis/libhiredis.a $(CFLAGS)

$(BUILD)/io_tests: test/io_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ $^ $(CFLAGS)

$(BUILD)/task_tests: test/task_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ $^ $(CFLAGS)

$(BUILD)/redis_tests: hiredis test/redis_tests.c $(BUILD)/libcommon.a logging.h
	$(CC) -o $@ test/redis_tests.c logging.c $(BUILD)/libcommon.a thirdparty/hiredis/libhiredis.a $(CFLAGS)

clean:
	rm -f *.o state/*.o test/*.o thirdparty/ae/*.o
	rm -rf $(BUILD)/*

redis:
	cd thirdparty ; bash ./build-redis.sh

hiredis:
	git submodule update --init --recursive -- "thirdparty/hiredis" ; cd thirdparty/hiredis ; make

test: hiredis redis $(BUILD)/common_tests $(BUILD)/db_tests $(BUILD)/io_tests $(BUILD)/task_tests $(BUILD)/redis_tests FORCE
	./thirdparty/redis-3.2.3/src/redis-server &
	sleep 1s ; ./build/common_tests ; ./build/db_tests ; ./build/io_tests ; ./build/task_tests ; ./build/redis_tests

valgrind: test
	valgrind --leak-check=full --error-exitcode=1 ./build/common_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/db_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/io_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/task_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/redis_tests

FORCE:

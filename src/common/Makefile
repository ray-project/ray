CC = gcc
CFLAGS = -g -Wall -Wextra -Werror=implicit-function-declaration -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter -Wno-type-limits -Wno-missing-field-initializers --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -fPIC -I. -Ithirdparty -Ithirdparty/ae
BUILD = build

all: hiredis redis redismodule $(BUILD)/libcommon.a

$(BUILD)/libcommon.a: event_loop.o common.o task.o io.o net.o logging.o state/redis.o state/table.o state/object_table.o state/task_table.o state/db_client_table.o state/local_scheduler_table.o thirdparty/ae/ae.o thirdparty/sha256.o
	ar rcs $@ $^

$(BUILD)/common_tests: test/common_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ test/common_tests.c $(BUILD)/libcommon.a $(CFLAGS)

$(BUILD)/db_tests: hiredis test/db_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ test/db_tests.c $(BUILD)/libcommon.a thirdparty/hiredis/libhiredis.a $(CFLAGS)

$(BUILD)/object_table_tests: hiredis test/object_table_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ test/object_table_tests.c $(BUILD)/libcommon.a thirdparty/hiredis/libhiredis.a $(CFLAGS)

$(BUILD)/task_table_tests: hiredis test/task_table_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ test/task_table_tests.c $(BUILD)/libcommon.a thirdparty/hiredis/libhiredis.a $(CFLAGS)

$(BUILD)/io_tests: test/io_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ $^ $(CFLAGS)

$(BUILD)/task_tests: test/task_tests.c $(BUILD)/libcommon.a
	$(CC) -o $@ $^ thirdparty/hiredis/libhiredis.a $(CFLAGS)

$(BUILD)/redis_tests: hiredis test/redis_tests.c $(BUILD)/libcommon.a logging.h
	$(CC) -o $@ test/redis_tests.c logging.c $(BUILD)/libcommon.a thirdparty/hiredis/libhiredis.a $(CFLAGS)

clean:
	rm -f *.o state/*.o test/*.o thirdparty/ae/*.o
	rm -rf $(BUILD)/*
	cd redis_module; make clean

redis:
	cd thirdparty ; bash ./build-redis.sh

hiredis:
	cd thirdparty/hiredis ; make

redismodule:
	cd redis_module && make && cd ..

test: CFLAGS += -DRAY_COMMON_LOG_LEVEL=4
test: hiredis redis redismodule $(BUILD)/common_tests $(BUILD)/task_table_tests $(BUILD)/object_table_tests $(BUILD)/db_tests $(BUILD)/io_tests $(BUILD)/task_tests $(BUILD)/redis_tests FORCE
	./thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./redis_module/ray_redis_module.so &
	sleep 1s
	./build/common_tests
	./build/db_tests
	./build/io_tests
	./build/task_tests
	./build/redis_tests
	./build/task_table_tests
	./build/object_table_tests
	./thirdparty/redis/src/redis-cli shutdown
	python ./redis_module/runtest.py

valgrind: test
	./thirdparty/redis/src/redis-server --loglevel warning --loadmodule redis_module/ray_redis_module.so &
	sleep 1s
	valgrind --leak-check=full --error-exitcode=1 ./build/common_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/db_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/io_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/task_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/redis_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/task_table_tests
	valgrind --leak-check=full --error-exitcode=1 ./build/object_table_tests
	./thirdparty/redis/src/redis-cli shutdown

FORCE:

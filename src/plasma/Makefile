CC = gcc
# The -rdynamic is here so we always get function names in backtraces.
CFLAGS = -g -Wall -rdynamic -Wextra -Werror=implicit-function-declaration -Wno-sign-compare -Wno-unused-parameter -Wno-type-limits -Wno-missing-field-initializers --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -I. -Ithirdparty -I../common -I../common/thirdparty -I../common/build/flatcc-prefix/src/flatcc/include
TEST_CFLAGS = -DPLASMA_TEST=1 -I.
BUILD = build

all: $(BUILD)/plasma_store $(BUILD)/plasma_manager $(BUILD)/plasma_client.so $(BUILD)/example $(BUILD)/libplasma_client.a

debug: FORCE
debug: CFLAGS += -DRAY_COMMON_LOG_LEVEL=0
debug: all

clean:
	cd ../common; make clean
	rm -f *.o
	rm -rf $(BUILD)/*

$(BUILD)/manager_tests: test/manager_tests.c plasma.h plasma.c plasma_client.h plasma_client.c thirdparty/xxhash.c plasma_protocol.h plasma_protocol.c plasma_manager.h plasma_manager.c fling.h fling.c common
	$(CC) $(CFLAGS) $(TEST_CFLAGS) -o $@ test/manager_tests.c plasma.c plasma_manager.c plasma_client.c thirdparty/xxhash.c plasma_protocol.c fling.c ../common/build/libcommon.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a ../common/thirdparty/hiredis/libhiredis.a

$(BUILD)/client_tests: test/client_tests.c plasma.h plasma.c plasma_client.h plasma_client.c thirdparty/xxhash.c plasma_protocol.h plasma_protocol.c plasma_manager.h plasma_manager.c fling.h fling.c common
	$(CC) $(CFLAGS) $(TEST_CFLAGS) -o $@ test/client_tests.c plasma.c plasma_manager.c plasma_client.c thirdparty/xxhash.c plasma_protocol.c fling.c ../common/build/libcommon.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a ../common/thirdparty/hiredis/libhiredis.a

$(BUILD)/serialization_tests: test/serialization_tests.c plasma.h plasma.c plasma_protocol.h plasma_protocol.c common
	$(CC) $(CFLAGS) $(TEST_CFLAGS) -o $@ test/serialization_tests.c plasma.c plasma_protocol.c ../common/build/libcommon.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a

$(BUILD)/plasma_store: plasma_store.c plasma.h plasma.c plasma_protocol.c eviction_policy.c fling.h fling.c malloc.c malloc.h thirdparty/dlmalloc.c common
	$(CC) $(CFLAGS) plasma_store.c plasma.c plasma_protocol.c eviction_policy.c fling.c malloc.c ../common/build/libcommon.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a -o $(BUILD)/plasma_store

$(BUILD)/plasma_manager: plasma_manager.c plasma.h plasma.c plasma_protocol.c plasma_client.c thirdparty/xxhash.c fling.h fling.c common
	$(CC) $(CFLAGS) plasma_manager.c plasma.c plasma_protocol.c plasma_client.c thirdparty/xxhash.c fling.c ../common/build/libcommon.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a ../common/thirdparty/hiredis/libhiredis.a -o $(BUILD)/plasma_manager

$(BUILD)/plasma_client.so: plasma.h plasma.c plasma_protocol.c plasma_client.c thirdparty/xxhash.c fling.h fling.c common
	$(CC) $(CFLAGS) plasma.c plasma_protocol.c plasma_client.c thirdparty/xxhash.c fling.c ../common/build/libcommon.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a -fPIC -shared -o $(BUILD)/plasma_client.so

$(BUILD)/libplasma_client.a: plasma.o plasma_protocol.o plasma_client.o thirdparty/xxhash.o fling.o ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a
	ar rcs $@ $^

$(BUILD)/example: plasma_client.c thirdparty/xxhash.c plasma.h plasma.c plasma_protocol.c example.c fling.h fling.c common
	$(CC) $(CFLAGS) plasma_client.c thirdparty/xxhash.c plasma.c plasma_protocol.c example.c fling.c ../common/build/libcommon.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a -o $(BUILD)/example

common: FORCE
	cd ../common; make; cd build; cmake ..; make; cd ../../plasma/build; cmake ..; make

# Set the request timeout low and logging level at FATAL for testing purposes.
test: CFLAGS += -DRAY_TIMEOUT=50 -DRAY_COMMON_LOG_LEVEL=4
# First, build and run all the unit tests.
test: $(BUILD)/manager_tests $(BUILD)/client_tests $(BUILD)/serialization_tests FORCE
	./build/plasma_store -s /tmp/plasma_store_socket_1 -m 0 &
	sleep 1
	./build/manager_tests
	killall plasma_store
	./build/serialization_tests
	./test/run_client_tests.sh
	cd ../common; make redis
# Next, build all the executables for Python testing.
test: all

valgrind: test
	./build/plasma_store -s /tmp/plasma_store_socket_1 -m 0 &
	sleep 1
	valgrind --leak-check=full --error-exitcode=1 ./build/manager_tests
	killall plasma_store
	valgrind --leak-check=full --error-exitcode=1 ./build/serialization_tests

FORCE:

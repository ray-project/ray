CC = gcc
CFLAGS = -g -Wall -Wextra -Werror=implicit-function-declaration -Wno-sign-compare -Wno-unused-parameter -Wno-type-limits -Wno-missing-field-initializers --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -I.. -I../common -I../common/thirdparty -I../plasma/ -fPIC
TEST_CFLAGS = -DPHOTON_TEST=1 -I.
BUILD = build

all: $(BUILD)/photon_scheduler $(BUILD)/photon_client.a

$(BUILD)/photon_tests: test/photon_tests.c photon.h photon_scheduler.h photon_scheduler.c photon_algorithm.h photon_algorithm.c photon_client.h photon_client.c common
	$(CC) $(CFLAGS) $(TEST_CFLAGS) -o $@ test/photon_tests.c photon_scheduler.c photon_algorithm.c photon_client.c ../common/build/libcommon.a ../common/thirdparty/hiredis/libhiredis.a -I../common/thirdparty/ -I../common/ ../plasma/build/libplasma_client.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a -I../plasma/

$(BUILD)/photon_client.a: photon_client.o
	ar rcs $(BUILD)/photon_client.a photon_client.o

$(BUILD)/photon_scheduler: photon.h photon_scheduler.c photon_algorithm.c common
	$(CC) $(CFLAGS) -o $@ photon_scheduler.c photon_algorithm.c ../common/build/libcommon.a ../common/thirdparty/hiredis/libhiredis.a -I../common/thirdparty/ -I../common/ ../plasma/build/libplasma_client.a ../common/build/flatcc-prefix/src/flatcc/lib/libflatcc.a

common: FORCE
	cd ../common; make; cd build; cmake ..; make

clean:
	cd ../common; make clean
	rm -rf $(BUILD)/*
	rm -f *.o

# Set the request timeout low and logging level at FATAL for testing purposes.
test: CFLAGS += -DRAY_TIMEOUT=50 -DRAY_COMMON_LOG_LEVEL=4
test: $(BUILD)/photon_tests FORCE
	../common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ../common/redis_module/ray_redis_module.so &
	../plasma/build/plasma_store -s /tmp/plasma_store_socket_1 -m 100000000 &
	sleep 0.5s
	./build/photon_tests
	../common/thirdparty/redis/src/redis-cli shutdown
	killall plasma_store

valgrind: test
	../common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ../common/redis_module/ray_redis_module.so &
	../plasma/build/plasma_store -s /tmp/plasma_store_socket_1 -m 100000000 &
	sleep 0.5s
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./build/photon_tests
	../common/thirdparty/redis/src/redis-cli shutdown
	killall plasma_store

FORCE:

CC = gcc
CFLAGS = -g -Wall --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -I. -I../common -I../common/thirdparty -I../common/state
BUILD = build

all: $(BUILD)/global_scheduler

$(BUILD)/global_scheduler: global_scheduler.c global_scheduler_algorithm.c
	$(CC) $(CFLAGS) -o $@ global_scheduler.c global_scheduler_algorithm.c ../common/build/libcommon.a ../common/thirdparty/hiredis/libhiredis.a

clean:
	rm -rf $(BUILD)/*

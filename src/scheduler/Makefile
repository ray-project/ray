CC = gcc
CFLAGS = -g -Wall --std=c99 -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE=200809L -I. -I../common -I../common/thirdparty
BUILD = build

$(BUILD)/scheduler: scheduler.c
	$(CC) $(CFLAGS) -o $@ scheduler.c ../common/build/libcommon.a ../common/thirdparty/hiredis/libhiredis.a

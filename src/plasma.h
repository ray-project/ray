#ifndef PLASMA_H
#define PLASMA_H

#include <inttypes.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#define LOG_ERR(M, ...) \
  fprintf(stderr, "[ERROR] (%s:%d: errno: %s) " M "\n", \
    __FILE__, __LINE__, errno == 0 ? "None" : strerror(errno), ##__VA_ARGS__)

#define LOG_INFO(M, ...) \
  fprintf(stderr, "[INFO] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)

typedef struct {
  int64_t size;
  int64_t create_time;
  int64_t construct_duration;
} plasma_object_info;

// Represents an object id hash, can hold a full SHA1 hash
typedef struct {
  unsigned char id[20];
} plasma_id;

enum plasma_request_type {
  PLASMA_CREATE, // create a new object
  PLASMA_GET, // get an object
  PLASMA_SEAL, // seal an object
  PLASMA_TRANSFER, // request transfer to another store
  PLASMA_DATA, // header for sending data
  PLASMA_REGISTER // register a plasma manager
};

typedef struct {
  int type;
  int manager_id;
  plasma_id object_id;
  int64_t size;
  uint8_t addr[4];
  int port;
} plasma_request;

enum plasma_reply_type {
  PLASMA_OBJECT, // the file descriptor represents an object
  PLASMA_FUTURE, // the file descriptor represents a future
};

typedef struct {
  int type;
  int64_t size;
} plasma_reply;

typedef struct {
  plasma_id object_id;
  void *data;
  int64_t size;
  int writable;
} plasma_buffer;

int plasma_store_connect(const char* socket_name);
plasma_buffer plasma_create(int conn, plasma_id object_id, int64_t size);
plasma_buffer plasma_get(int conn, plasma_id object_id);
void plasma_seal(int fd, plasma_id object_id);

#endif

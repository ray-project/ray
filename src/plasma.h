#ifndef PLASMA_H
#define PLASMA_H

#include <inttypes.h>
#include <stdio.h>
#include <errno.h>
#include <stddef.h>
#include <string.h>

#ifdef NDEBUG
#define LOG_DEBUG(M, ...)
#else
#define LOG_DEBUG(M, ...) \
  fprintf(stderr, "[DEBUG] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#endif

#ifdef PLASMA_LOGGIN_ON
#define LOG_INFO(M, ...) \
  fprintf(stderr, "[INFO] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
#define LOG_INFO(M, ...)
#endif

#define LOG_ERR(M, ...)                                                     \
  fprintf(stderr, "[ERROR] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, \
          errno == 0 ? "None" : strerror(errno), ##__VA_ARGS__)

#define PLASMA_CHECK(CONDITION, M, ...)                                \
  do {                                                                 \
    if (!(CONDITION)) {                                                \
      fprintf(stderr, "[FATAL] (%s:%d " #CONDITION ") \n" M, __FILE__, \
              __LINE__);                                               \
      exit(-1);                                                        \
    }                                                                  \
  } while (0)

typedef struct {
  int64_t data_size;
  int64_t metadata_size;
  int64_t create_time;
  int64_t construct_duration;
} plasma_object_info;

/* Represents an object id hash, can hold a full SHA1 hash */
typedef struct { unsigned char id[20]; } plasma_id;

enum plasma_request_type {
  /* Create a new object. */
  PLASMA_CREATE,
  /* Get an object. */
  PLASMA_GET,
  /* seal an object */
  PLASMA_SEAL,
  /* request transfer to another store */
  PLASMA_TRANSFER,
  /* Header for sending data */
  PLASMA_DATA,
};

typedef struct {
  int type;
  plasma_id object_id;
  /* The size of the data. */
  int64_t data_size;
  /* The size of the metadata. */
  int64_t metadata_size;
  uint8_t addr[4];
  int port;
} plasma_request;

typedef struct {
  /* The offset in the memory mapped file of the data. */
  ptrdiff_t data_offset;
  /* The offset in the memory mapped file of the metadata. */
  ptrdiff_t metadata_offset;
  /* The size of the memory mapped file. */
  int64_t map_size;
  /* The size of the data. */
  int64_t data_size;
  /* The size of the metadata. */
  int64_t metadata_size;
} plasma_reply;

typedef struct {
  plasma_id object_id;
  uint8_t *data;
  int64_t data_size;
  uint8_t *metadata;
  int64_t metadata_size;
  int writable;
} plasma_buffer;

/* Connect to the local plasma store UNIX domain socket */
int plasma_store_connect(const char *socket_name);

/* Connect to a possibly remote plasma manager */
int plasma_manager_connect(const char *addr, int port);

void plasma_create(int conn,
                   plasma_id object_id,
                   int64_t size,
                   uint8_t *metadata,
                   int64_t metadata_size,
                   uint8_t **data);
void plasma_get(int conn,
                plasma_id object_id,
                int64_t *size,
                uint8_t **data,
                int64_t *metadata_size,
                uint8_t **metadata);
void plasma_seal(int store, plasma_id object_id);

void plasma_send(int conn, plasma_request *req);

#endif

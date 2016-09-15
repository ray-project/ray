#ifndef PLASMA_H
#define PLASMA_H

#include <inttypes.h>
#include <stdio.h>
#include <errno.h>
#include <stddef.h>
#include <string.h>

#include "uthash.h"

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
  /* Numerical value of the fd of the memory mapped file in the store. */
  int store_fd_val;
} plasma_reply;

typedef struct {
  plasma_id object_id;
  uint8_t *data;
  int64_t data_size;
  uint8_t *metadata;
  int64_t metadata_size;
  int writable;
} plasma_buffer;

typedef struct {
  /* Key that uniquely identifies the  memory mapped file. In practice, we
   * take the numerical value of the file descriptor in the object store. */
  int key;
  /* The result of mmap for this file descriptor. */
  uint8_t *pointer;
  /* Handle for the uthash table. */
  UT_hash_handle hh;
} client_mmap_table_entry;

/* A client connection with a plasma store */
typedef struct {
  /* File descriptor of the Unix domain socket that connects to the store. */
  int conn;
  /* Table of dlmalloc buffer files that have been memory mapped so far. */
  client_mmap_table_entry *mmap_table;
} plasma_store_conn;

void plasma_send(int conn, plasma_request *req);

#endif

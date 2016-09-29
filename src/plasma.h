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

/** Represents an object ID hash, can hold a full SHA1 hash. */
typedef struct { unsigned char id[20]; } plasma_id;

enum plasma_request_type {
  /** Create a new object. */
  PLASMA_CREATE,
  /** Get an object. */
  PLASMA_GET,
  /** Check if an object is present. */
  PLASMA_CONTAINS,
  /** Seal an object. */
  PLASMA_SEAL,
  /** Delete an object. */
  PLASMA_DELETE,
  /** Request transfer to another store. */
  PLASMA_TRANSFER,
  /** Header for sending data. */
  PLASMA_DATA,
};

typedef struct {
  /** The type of the request. */
  int type;
  /** The ID of the object that the request is about. */
  plasma_id object_id;
  /** The size of the object's data. */
  int64_t data_size;
  /** The size of the object's metadata. */
  int64_t metadata_size;
  /** In a transfer request, this is the IP address of the Plasma Manager to
   *  transfer the object to. */
  uint8_t addr[4];
  /** In a transfer request, this is the port of the Plasma Manager to transfer
   *  the object to. */
  int port;
} plasma_request;

typedef struct {
  /** The offset in bytes in the memory mapped file of the data. */
  ptrdiff_t data_offset;
  /** The offset in bytes in the memory mapped file of the metadata. */
  ptrdiff_t metadata_offset;
  /** The size in bytes of the memory mapped file. */
  int64_t map_size;
  /** The size in bytesof the data. */
  int64_t data_size;
  /** The size in bytes of the metadata. */
  int64_t metadata_size;
  /** This is used only to respond to requests of type PLASMA_CONTAINS. It is 1
   *  if the object is present and 0 otherwise. Used for plasma_contains. */
  int has_object;
  /** The file descriptor of the memory mapped file in the store. */
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
  /** Key that uniquely identifies the  memory mapped file. In practice, we
   *  take the numerical value of the file descriptor in the object store. */
  int key;
  /** The result of mmap for this file descriptor. */
  uint8_t *pointer;
  /** Handle for the uthash table. */
  UT_hash_handle hh;
} client_mmap_table_entry;

/** Information about a connection between a Plasma Client and Plasma Store.
 *  This is used to avoid mapping the same files into memory multiple times. */
typedef struct {
  /** File descriptor of the Unix domain socket that connects to the store. */
  int conn;
  /** Table of dlmalloc buffer files that have been memory mapped so far. */
  client_mmap_table_entry *mmap_table;
} plasma_store_conn;

/**
 * This is used by the Plasma Client to send a request to the Plasma Store or
 * the Plasma Manager.
 *
 * @param conn The file descriptor to use to send the request.
 * @param req The address of the request to send.
 * @return Void.
 */
void plasma_send_request(int conn, plasma_request *req);

/**
 * This is used by the Plasma Store to send a reply to the Plasma Client.
 *
 * @param conn The file descriptor to use to send the reply.
 * @param req The address of the reply to send.
 * @return Void.
 */
void plasma_send_reply(int conn, plasma_reply *req);

#endif

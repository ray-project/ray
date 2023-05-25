
#ifndef __RAY_H__
#define __RAY_H__

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define RAY_OBJECT_ID_MAGIC 0xc0de550a

#define RAY_TYPE_INVALID 0x0
#define RAY_TYPE_OBJECT_ID 0x1
#define RAY_TYPE_DATA 0x2

#define RAY_FLAG_NEED_DEALLOC 0x1 << 31

typedef struct {
  unsigned int magic;     // offset 0
  unsigned int type;      // offset 4
  unsigned int flags;     // offset 8
  unsigned char *buf;     // offset 12
  unsigned int len;       // offset 16
  unsigned int cap;       // offset 20
  unsigned int checksum;  // offset 24
} ray_buffer;

__attribute__((import_module("ray"), import_name("test"))) void test();

__attribute__((import_module("ray"), import_name("sleep"))) void sleep(int);

__attribute__((import_module("ray"), import_name("init"))) int rinit();

__attribute__((import_module("ray"), import_name("call"))) int rcall(ray_buffer *id,
                                                                     void *func,
                                                                     ...);

__attribute__((import_module("ray"), import_name("get"))) int rget(ray_buffer *id,
                                                                   unsigned char *buf,
                                                                   unsigned int *len);

__attribute__((import_module("ray"), import_name("put"))) int rput(ray_buffer *id,
                                                                   unsigned char *buf,
                                                                   unsigned int len);

static inline unsigned int calculate_checksum(ray_buffer *);

static inline ray_buffer *alloc_ray_buffer(unsigned int type, unsigned int cap) {
  ray_buffer *buf = (ray_buffer *)malloc(sizeof(ray_buffer));
  if (buf == NULL) {
    return NULL;
  }

  unsigned char *ibuf = (unsigned char *)malloc(cap);
  if (ibuf == NULL) {
    free(buf);
    return NULL;
  }

  buf->magic = RAY_OBJECT_ID_MAGIC;
  buf->type = type;
  buf->buf = ibuf;
  buf->len = 0;
  buf->cap = cap;
  buf->checksum = calculate_checksum(buf);
  return buf;
}

static inline unsigned int calculate_checksum(ray_buffer *buf) {
  return (unsigned int)(buf->magic ^ buf->type ^ buf->flags ^ buf->len ^ buf->cap ^
                        (unsigned int)buf->buf);
}

static inline void free_ray_buffer(ray_buffer *buf) {
  if (buf != NULL) {
    if (buf->buf != NULL) {
      free(buf->buf);
    }
    free(buf);
  }
}

static inline int validate_ray_buffer(ray_buffer *buf) {
  if (buf == NULL) {
    return 1;
  }
  if (buf->magic != RAY_OBJECT_ID_MAGIC) {
    return 1;
  }
  if (buf->checksum != calculate_checksum(buf)) {
    return 1;
  }
  return 0;
}

static inline void *_alloc_ray_buffer_with_data(unsigned int type,
                                                unsigned char *data,
                                                unsigned int len) {
  ray_buffer *buf = alloc_ray_buffer(type, len);
  if (buf == NULL) {
    return NULL;
  }
  memcpy(buf->buf, data, len);
  buf->len = len;
  buf->flags = RAY_FLAG_NEED_DEALLOC;
  buf->checksum = calculate_checksum(buf);
  return buf;
}

static inline int _ray_call(ray_buffer *id, void *func, int count, ...) {
  va_list args;
  va_start(args, count);
  int res = rcall(id, func, args);
  for (int i = 0; i < count; i++) {
    void *arg = va_arg(args, void *);
    // skip if not valid ray_buffer
    if (validate_ray_buffer(arg)) {
      continue;
    }
    // free if needed
    if (((ray_buffer *)arg)->flags & RAY_FLAG_NEED_DEALLOC) {
      free_ray_buffer(arg);
    }
  }
  va_end(args);
  // free all buffers
  return res;
}

#define R_STRUCT_VAR(name) \
  _alloc_ray_buffer_with_data(RAY_TYPE_DATA, (unsigned char *)&(name), sizeof(name))

#define R_ASYNC_CALL(...) _ray_call(__VA_ARGS__)

#endif  // __RAY_H__

#ifndef OBJECT_H
#define OBJECT_H

#include <stdint.h>

#include "common.h"

/**
 * Object information data structure.
 */
typedef struct {
  object_id obj_id;
  int64_t data_size;
  int64_t metadata_size;
  int64_t create_time;
  int64_t construct_duration;
  unsigned char digest[DIGEST_SIZE];
} object_info;

void object_id_print(object_id obj_id);
char * object_id_tostring(object_id obj_id, char *, int);

#endif

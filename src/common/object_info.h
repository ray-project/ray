#ifndef OBJECT_H
#define OBJECT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "common.h"
#include "utstring.h"
/**
 * Object information data structure.
 */
typedef struct {
  object_id objid;
  int64_t data_size;
  int64_t metadata_size;
  int64_t create_time;
  int64_t construct_duration;
} object_info;

typedef object_info  plasma_object_info;

#endif

#ifndef OBJECT_H
#define OBJECT_H
#include "common.h"
#include "object_info.h"

#include <stdio.h>

void object_id_print(object_id obj_id) {
  for (int i = 0; i < sizeof(object_id); ++i) {
    printf("%02x", (uint8_t) obj_id.id[i]);
    if (i < sizeof(object_id) - 1) {
      printf(".");
    }
  }
}

#endif

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

/**
 * Creates a hex string(dot-delimited bytes) representation of the given obj_id.
 * @param obj_id object id
 * @return dot-delimited hex string representation of the object id.
 */
char * object_id_tostring(object_id obj_id, char *outstr, int outstrlen) {
  int offset = 0;
  if (outstrlen < 3*sizeof(obj_id)) {
    /*  need 3 characters per byte. */
    return NULL;
  }
  memset(outstr, 0, 2*sizeof(obj_id)); /* This should null terminate it. */
  for (int i = 0; i < sizeof(object_id); ++i) {
    sprintf(outstr + offset, "%02x", (uint8_t) obj_id.id[i]);
    offset += 2;
    if (i < sizeof(object_id) - 1) {
      sprintf(outstr + offset, ".");
      offset += 1;
    }
  }
  return outstr;
}

#endif

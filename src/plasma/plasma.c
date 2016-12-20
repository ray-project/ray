#include "plasma.h"

#include "io.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include "plasma_protocol.h"

bool plasma_object_ids_distinct(int num_object_ids, object_id object_ids[]) {
  for (int i = 0; i < num_object_ids; ++i) {
    for (int j = 0; j < i; ++j) {
      if (object_ids_equal(object_ids[i], object_ids[j])) {
        return false;
      }
    }
  }
  return true;
}

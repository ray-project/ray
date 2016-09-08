/* A simple example on how to use the plasma store
 *
 * Can be called in the following way:
 *
 * cd build
 * ./plasma_store -s /tmp/plasma_socket
 * ./example -s /tmp/plasma_socket -g
 * ./example -s /tmp/plasma_socket -c -f */

#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <assert.h>

#include "plasma.h"

int main(int argc, char *argv[]) {
  int conn = -1;
  int64_t size;
  void *data;
  int c;
  plasma_id id = {{255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                   255, 255, 255, 255, 255, 255, 255, 255, 255, 255}};
  while ((c = getopt(argc, argv, "s:cfg")) != -1) {
    switch (c) {
    case 's':
      conn = plasma_store_connect(optarg);
      break;
    case 'c':
      assert(conn != -1);
      plasma_create(conn, id, 100, &data);
      break;
    case 'f':
      assert(conn != -1);
      plasma_seal(conn, id);
      break;
    case 'g':
      plasma_get(conn, id, &size, &data);
      break;
    default:
      abort();
    }
  }
  assert(conn != -1);
  close(conn);
}

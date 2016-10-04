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
#include "plasma_client.h"

int main(int argc, char *argv[]) {
  plasma_store_conn *conn = NULL;
  int64_t size;
  uint8_t *data;
  int c;
  object_id id = {{255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                   255, 255, 255, 255, 255, 255, 255, 255, 255, 255}};
  while ((c = getopt(argc, argv, "s:cfg")) != -1) {
    switch (c) {
    case 's':
      conn = plasma_store_connect(optarg);
      break;
    case 'c':
      assert(conn != NULL);
      plasma_create(conn, id, 100, NULL, 0, &data);
      break;
    case 'f':
      assert(conn != NULL);
      plasma_seal(conn, id);
      break;
    case 'g':
      plasma_get(conn, id, &size, &data, NULL, NULL);
      break;
    default:
      abort();
    }
  }
  assert(conn != NULL);
  plasma_store_disconnect(conn);
}

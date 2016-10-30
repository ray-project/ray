#include <getopt.h>
#include <stdlib.h>

#include "state/db.h"

#include "common.h"
#include "event_loop.h"

typedef struct {
  /** The global scheduler event loop. */
  event_loop *loop;
  /** The global state store database. */
  db_handle *db;
} global_scheduler_state;

global_scheduler_state *init_global_scheduler(event_loop *loop,
                                              const char *redis_addr,
                                              int redis_port) {
  global_scheduler_state *state = malloc(sizeof(global_scheduler_state));
}

void start_server(const char* redis_addr_port) {

}

int main(int argc, char *argv[]) {
  /* IP address and port of redis. */
  char *redis_addr_port = NULL;
  int c;
  while ((c = getopt(argc, argv, "s:m:h:p:r:")) != -1) {
    switch (c) {
    case 'r':
      redis_addr_port = optarg;
      break;
    default:
      LOG_ERR("unknown option %c", c);
      exit(-1);
    }
  }
  start_server(redis_addr_port);
}

#include <getopt.h>
#include <stdlib.h>

#include "state/db.h"

#include "common.h"
#include "event_loop.h"
#include "net.h"

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
  state->db = db_connect(redis_addr, redis_port, "global_scheduler", "", -1);
  db_attach(state->db, loop);
  return state;
}

void start_server(const char* redis_addr, int redis_port) {
  event_loop *loop = event_loop_create();
  global_scheduler_state *state = init_global_scheduler(loop, redis_addr, redis_port);
  event_loop_run(loop);
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
  char redis_addr[16];
  int redis_port;
  if (!redis_addr_port ||
      parse_ip_addr_port(redis_addr_port, redis_addr, &redis_port) == -1) {
    LOG_ERR("need to specify redis address like 127.0.0.1:6379 with -r switch");
    exit(-1);
  }
  start_server(redis_addr, redis_port);
}

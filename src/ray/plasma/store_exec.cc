#include "plasma/store.h"
#include <getopt.h>
#include <stdio.h>

int main(int argc, char* argv[]) {
  std::string socket_name;
  // Directory where plasma memory mapped files are stored.
  std::string plasma_directory;
  std::string external_store_endpoint;
  bool hugepages_enabled = false;
  int64_t system_memory = -1;
  int c;
  while ((c = getopt(argc, argv, "s:m:d:e:h")) != -1) {
    switch (c) {
      case 'd':
        plasma_directory = std::string(optarg);
        break;
      case 'e':
        external_store_endpoint = std::string(optarg);
        break;
      case 'h':
        hugepages_enabled = true;
        break;
      case 's':
        socket_name = std::string(optarg);
        break;
      case 'm': {
        char extra;
        int scanned = sscanf(optarg, "%" SCNd64 "%c", &system_memory, &extra);
        ARROW_CHECK(scanned == 1);
        break;
      }
      default:
        exit(-1);
    }
  }

  plasma::PlasmaStoreRunner runner(
    socket_name, system_memory, hugepages_enabled, plasma_directory, external_store_endpoint);
  runner.Start();
  return 0;
}

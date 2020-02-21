#include "ray/gcs/gcs_client.h"

namespace ray {
namespace gcs {

GcsClientOptions::GcsClientOptions(const std::string &ip, int port,
                                   const std::string &password, bool is_test_client)
    : server_ip_(ip),
      server_port_(port),
      password_(password),
      is_test_client_(is_test_client) {}

}  // namespace gcs
}  // namespace ray

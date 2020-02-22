#include "ray/gcs/gcs_client.h"

namespace ray {
namespace gcs {

std::string gcs::GcsClient::DebugString() const { return ""; }

ActorInfoAccessor &gcs::GcsClient::Actors() {
  RAY_CHECK(actor_accessor_ != nullptr);
  return *actor_accessor_;
}

JobInfoAccessor &gcs::GcsClient::Jobs() {
  RAY_CHECK(job_accessor_ != nullptr);
  return *job_accessor_;
}

ObjectInfoAccessor &gcs::GcsClient::Objects() {
  RAY_CHECK(object_accessor_ != nullptr);
  return *object_accessor_;
}

NodeInfoAccessor &gcs::GcsClient::Nodes() {
  RAY_CHECK(node_accessor_ != nullptr);
  return *node_accessor_;
}

TaskInfoAccessor &gcs::GcsClient::Tasks() {
  RAY_CHECK(task_accessor_ != nullptr);
  return *task_accessor_;
}

ErrorInfoAccessor &gcs::GcsClient::Errors() {
  RAY_CHECK(error_accessor_ != nullptr);
  return *error_accessor_;
}

StatsInfoAccessor &gcs::GcsClient::Stats() {
  RAY_CHECK(stats_accessor_ != nullptr);
  return *stats_accessor_;
}

WorkerInfoAccessor &gcs::GcsClient::Workers() {
  RAY_CHECK(worker_accessor_ != nullptr);
  return *worker_accessor_;
}

GcsClientOptions::GcsClientOptions(const std::string &ip, int port,
                                   const std::string &password, bool is_test_client)
    : server_ip_(ip),
      server_port_(port),
      password_(password),
      is_test_client_(is_test_client) {}

GcsClient::~GcsClient() {}

GcsClient::GcsClient(const GcsClientOptions &options) : options_(options) {}

}  // namespace gcs
}  // namespace ray

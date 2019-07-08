#include "ray/gcs/actor_state_accessor.h"
#include <boost/none.hpp>
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

ActorStateAccessor::ActorStateAccessor(RedisGcsClient &client_impl)
    : client_impl_(client_impl) {}

Status ActorStateAccessor::AsyncGet(const JobID &job_id, const ActorID &actor_id,
                                    const MultiItemCallback<ActorTableData> &callback) {
  RAY_DCHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                            const std::vector<ActorTableData> &data) {
    callback(Status::OK(), data);
  };

  ActorTable &actor_table = client_impl_.actor_table();
  return actor_table.Lookup(job_id, actor_id, on_done);
}

Status ActorStateAccessor::AsyncAdd(const JobID &job_id, const ActorID &actor_id,
                                    std::shared_ptr<ActorTableData> data_ptr,
                                    const StatusCallback &callback) {
  // The actor log starts with an ALIVE entry. This is followed by 0 to N pairs
  // of (RECONSTRUCTING, ALIVE) entries, where N is the maximum number of
  // reconstructions. This is followed optionally by a DEAD entry.
  int log_length =
      2 * (data_ptr->max_reconstructions() - data_ptr->remaining_reconstructions());
  if (data_ptr->state() != ActorTableData::ALIVE) {
    // RECONSTRUCTING or DEAD entries have an odd index.
    log_length += 1;
  }

  ActorTable &actor_table = client_impl_.actor_table();
  if (callback != nullptr) {
    auto on_success = [callback, data_ptr](
                          RedisGcsClient *client, const ActorID &actor_id,
                          const ActorTableData &data) { callback(Status::OK()); };

    auto on_failure = [callback, data_ptr](RedisGcsClient *client,
                                           const ActorID &actor_id,
                                           const ActorTableData &data) {
      callback(Status::Invalid("Add failed, maybe exceeds max reconstruct number."));
    };

    return actor_table.AppendAt(job_id, actor_id, data_ptr, on_success, on_failure,
                                log_length);
  }

  return actor_table.AppendAt(job_id, actor_id, data_ptr, nullptr, nullptr, log_length);
}

Status ActorStateAccessor::AsyncSubscribe(
    const JobID &job_id, const ClientID &client_id,
    const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_DCHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](RedisGcsClient *client, const ActorID &actor_id,
                                  const std::vector<ActorTableData> &data) {
    subscribe(actor_id, data);
  };

  auto on_done = [done](RedisGcsClient *client) {
    if (done != nullptr) {
      done(Status::OK());
    }
  };

  ActorTable &actor_table = client_impl_.actor_table();
  return actor_table.Subscribe(job_id, client_id, on_subscribe, on_done);
}

}  // namespace gcs

}  // namespace ray

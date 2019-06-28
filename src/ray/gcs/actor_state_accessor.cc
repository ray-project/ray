#include "ray/gcs/actor_state_accessor.h"
#include <boost/none.hpp>
#include "ray/gcs/client.h"
#include "ray/gcs/gcs_client_impl.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

ActorStateAccessor::ActorStateAccessor(GcsClientImpl &client_impl)
    : client_impl_(client_impl) {}

Status ActorStateAccessor::AsyncGet(const JobID &job_id, const ActorID &actor_id,
                                    const MultiItemCallback<ActorTableData> &callback) {
  RAY_DCHECK(callback != nullptr);
  auto on_done = [callback](AsyncGcsClient *client, const ActorID &actor_id,
                            const std::vector<ActorTableData> &data) {
    callback(Status::OK(), data);
  };

  ActorTable &actor_table = client_impl_.AsyncClient().actor_table();
  return actor_table.Lookup(job_id, actor_id, on_done);
}

Status ActorStateAccessor::AsyncAdd(const JobID &job_id, const ActorID &actor_id,
                                    std::shared_ptr<ActorTableData> data_ptr,
                                    size_t log_length, const StatusCallback &callback) {
  ActorTable &actor_table = client_impl_.AsyncClient().actor_table();
  if (callback != nullptr) {
    auto on_success = [callback, data_ptr](
        AsyncGcsClient *client, const ActorID &actor_id, const ActorTableData &data) {
      callback(Status::OK());
    };

    auto on_failure = [callback, data_ptr](
        AsyncGcsClient *client, const ActorID &actor_id, const ActorTableData &data) {
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
  auto on_subscribe = [subscribe](AsyncGcsClient *client, const ActorID &actor_id,
                                  const std::vector<ActorTableData> &data) {
    subscribe(actor_id, data);
  };

  auto on_done = [done](AsyncGcsClient *client) {
    if (done != nullptr) {
      done(Status::OK());
    }
  };

  ActorTable &actor_table = client_impl_.AsyncClient().actor_table();
  return actor_table.Subscribe(job_id, client_id, on_subscribe, on_done);
}

Status ActorStateAccessor::RequestNotifications(const JobID &job_id,
                                                const ActorID &actor_id,
                                                const ClientID &client_id) {
  ActorTable &actor_table = client_impl_.AsyncClient().actor_table();
  actor_table.RequestNotifications(job_id, actor_id, client_id);
  return Status::OK();
}

Status ActorStateAccessor::CancelNotifications(const JobID &job_id,
                                               const ActorID &actor_id,
                                               const ClientID &client_id) {
  ActorTable &actor_table = client_impl_.AsyncClient().actor_table();
  actor_table.CancelNotifications(job_id, actor_id, client_id);
  return Status::OK();
}

Status ActorStateAccessor::AsyncGetCheckpointIds(
    const JobID &job_id, const ActorID &actor_id,
    const OptionalItemCallback<ActorCheckpointIdData> &callback) {
  RAY_DCHECK(callback != nullptr);
  auto on_success = [callback](AsyncGcsClient *client, const ActorID &actor_id,
                               const ActorCheckpointIdData &data) {
    boost::optional<ActorCheckpointIdData> result(data);
    callback(Status::OK(), std::move(result));
  };

  auto on_failure = [callback](AsyncGcsClient *client, const ActorID &actor_id) {
    boost::optional<ActorCheckpointIdData> result;
    callback(Status::KeyError("JobID or ActorID not exist."), std::move(result));
  };

  ActorCheckpointIdTable &checkpoint_id_table =
      client_impl_.AsyncClient().actor_checkpoint_id_table();
  return checkpoint_id_table.Lookup(job_id, actor_id, on_success, on_failure);
}

}  // namespace gcs

}  // namespace ray

#include "ray/gcs/actor_state_accessor.h"
#include <boost/none.hpp>
#include "ray/gcs/client.h"
#include "ray/gcs/gcs_client_impl.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

ActorStateAccessor::ActorStateAccessor(GcsClientImpl &client_impl)
    : client_impl_(client_impl) {}

Status ActorStateAccessor::AsyncGet(
    const DriverID &driver_id, const ActorID &actor_id,
    const DatumCallback<ActorTableData>::OptionalItem &callback) {
  auto inner_callback = [callback](AsyncGcsClient *client, const ActorID &actor_id,
                                   const std::vector<ActorTableData> &data) {
    boost::optional<ActorTableData> result;
    if (!data.empty()) {
      RAY_CHECK(data.size() == 1);
      result = data[0];
    }
    callback(Status::OK(), std::move(result));
  };

  ActorTable &actor_table = client_impl_.AsyncClient().actor_table();
  return actor_table.Lookup(driver_id, actor_id, inner_callback);
}

Status ActorStateAccessor::AsyncAdd(const DriverID &driver_id, const ActorID &actor_id,
                                    std::shared_ptr<ActorTableData> data_ptr,
                                    size_t log_length, const StatusCallback &callback) {
  auto on_successed = [callback, data_ptr](
      AsyncGcsClient *client, const ActorID &actor_id, const ActorTableData &data) {
    callback(Status::OK());
  };

  auto on_failed = [callback, data_ptr](AsyncGcsClient *client, const ActorID &actor_id,
                                        const ActorTableData &data) {
    callback(Status::Invalid("Redis return error, maybe exceed max reconstruction"));
  };

  ActorTable &actor_table = client_impl_.AsyncClient().actor_table();
  return actor_table.AppendAt(driver_id, actor_id, data_ptr, on_successed, on_failed,
                              log_length);
}

Status ActorStateAccessor::AsyncSubscribe(
    const DriverID &driver_id, const DatumCallback<ActorTableData>::MultiItem &subscribe,
    const StatusCallback &done) {
  auto on_subscribe = [subscribe](AsyncGcsClient *client, const ActorID &actor_id,
                                  const std::vector<ActorTableData> &data) {
    subscribe(Status::OK(), data);
  };

  auto on_done = [done](AsyncGcsClient *client) { done(Status::OK()); };

  const ClientID &client_id = client_impl_.GetClientInfo().id_;
  ActorTable &actor_table = client_impl_.AsyncClient().actor_table();
  return actor_table.Subscribe(driver_id, client_id, on_subscribe, on_done);
}

Status ActorStateAccessor::AsyncGetCheckpointIds(
    const DriverID &driver_id, const ActorID &actor_id,
    const DatumCallback<ActorCheckpointIdData>::OptionalItem &callback) {
  auto on_successed = [callback](AsyncGcsClient *client, const ActorID &actor_id,
                                 const ActorCheckpointIdData &data) {
    boost::optional<ActorCheckpointIdData> result(data);
    callback(Status::OK(), std::move(result));
  };

  auto on_failed = [callback](AsyncGcsClient *client, const ActorID &actor_id) {
    boost::optional<ActorCheckpointIdData> result;
    callback(Status::KeyError("DriverID or ActorID not exist."), std::move(result));
  };

  ActorCheckpointIdTable &checkpoint_id_table =
      client_impl_.AsyncClient().actor_checkpoint_id_table();
  return checkpoint_id_table.Lookup(driver_id, actor_id, on_successed, on_failed);
}

}  // namespace gcs

}  // namespace ray

#include "ray/gcs/actor_state_accessor.h"
#include <boost/none.hpp>
#include "ray/gcs/client.h"
#include "ray/gcs/tables.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

ActorStateAccessor::ActorStateAccessor(AsyncGcsClient *client_impl)
    : client_impl_(client_impl) {
  RAY_CHECK(client_impl != nullptr);
}

Status ActorStateAccessor::AsyncGet(const DriverID &driver_id, const ActorID &actor_id,
                                    DatumCallback<ActorTableDataT>::SingleItem callback) {
  auto inner_callback = [callback](AsyncGcsClient *client, const ActorID &actor_id,
                                   const std::vector<ActorTableDataT> &data) {
    boost::optional<ActorTableDataT> result;
    if (!data.empty()) {
      RAY_CHECK(data.size() == 1);
      result = data[0];
    }
    callback(Status::OK(), std::move(result));
  };

  ActorTable &actor_table = client_impl_->actor_table();
  return actor_table.Lookup(driver_id, actor_id, inner_callback);
}

Status ActorStateAccessor::AsyncGetCheckpointIds(
    const DriverID &driver_id, const ActorID &actor_id,
    DatumCallback<ActorCheckpointIdDataT>::SingleItem callback) {
  auto on_successed = [callback](AsyncGcsClient *client, const ActorID &actor_id,
                                 const ActorCheckpointIdDataT &data) {
    boost::optional<ActorCheckpointIdDataT> result(data);
    callback(Status::OK(), std::move(result));
  };

  auto on_failed = [callback](AsyncGcsClient *client, const ActorID &actor_id) {
    boost::optional<ActorCheckpointIdDataT> result;
    callback(Status::KeyError("DriverID or ActorID not exist."), std::move(result));
  };

  ActorCheckpointIdTable &checkpoint_id_table = client_impl_->actor_checkpoint_id_table();
  return checkpoint_id_table.Lookup(driver_id, actor_id, on_successed, on_failed);
}

}  // namespace gcs

}  // namespace ray

#include "ray/gcs/actor_state_accessor.h"
#include <boost/none.hpp>
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

ActorStateAccessor::ActorStateAccessor(RedisGcsClient &client_impl)
    : client_impl_(client_impl), actor_sub_executor_(client_impl_.actor_table()) {}

Status ActorStateAccessor::AsyncGet(
    const ActorID &actor_id, const OptionalItemCallback<ActorTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                            const std::vector<ActorTableData> &data) {
    boost::optional<ActorTableData> result;
    if (!data.empty()) {
      result = data.back();
    }
    callback(Status::OK(), result);
  };

  ActorTable &actor_table = client_impl_.actor_table();
  return actor_table.Lookup(JobID::Nil(), actor_id, on_done);
}

Status ActorStateAccessor::AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                                         const StatusCallback &callback) {
  auto on_success = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
    if (callback != nullptr) {
      callback(Status::OK());
    }
  };

  auto on_failure = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
    if (callback != nullptr) {
      callback(Status::Invalid("Adding actor failed."));
    }
  };

  ActorID actor_id = ActorID::FromBinary(data_ptr->actor_id());
  ActorTable &actor_table = client_impl_.actor_table();
  return actor_table.AppendAt(JobID::Nil(), actor_id, data_ptr, on_success, on_failure,
                              /*log_length*/ 0);
}

Status ActorStateAccessor::AsyncUpdate(const ActorID &actor_id,
                                       const std::shared_ptr<ActorTableData> &data_ptr,
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

  auto on_success = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
    // If we successfully appended a record to the GCS table of the actor that
    // has died, signal this to anyone receiving signals from this actor.
    if (data.state() == ActorTableData::DEAD ||
        data.state() == ActorTableData::RECONSTRUCTING) {
      std::vector<std::string> args = {"XADD", actor_id.Hex(), "*", "signal",
                                       "ACTOR_DIED_SIGNAL"};
      auto redis_context = client->primary_context();
      RAY_CHECK_OK(redis_context->RunArgvAsync(args));
    }

    if (callback != nullptr) {
      callback(Status::OK());
    }
  };

  auto on_failure = [callback](RedisGcsClient *client, const ActorID &actor_id,
                               const ActorTableData &data) {
    if (callback != nullptr) {
      callback(Status::Invalid("Updating actor failed."));
    }
  };

  ActorTable &actor_table = client_impl_.actor_table();
  return actor_table.AppendAt(JobID::Nil(), actor_id, data_ptr, on_success, on_failure,
                              log_length);
}

Status ActorStateAccessor::AsyncSubscribe(
    const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return actor_sub_executor_.AsyncSubscribe(ClientID::Nil(), subscribe, done);
}

Status ActorStateAccessor::AsyncSubscribe(
    const ActorID &actor_id, const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return actor_sub_executor_.AsyncSubscribe(node_id_, actor_id, subscribe, done);
}

Status ActorStateAccessor::AsyncUnsubscribe(const ActorID &actor_id,
                                            const StatusCallback &done) {
  return actor_sub_executor_.AsyncUnsubscribe(node_id_, actor_id, done);
}

}  // namespace gcs

}  // namespace ray

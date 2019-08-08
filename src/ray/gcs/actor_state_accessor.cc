#include "ray/gcs/actor_state_accessor.h"
#include <boost/none.hpp>
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

ActorStateAccessor::ActorStateAccessor(RedisGcsClient &client_impl,
                                       boost::asio::io_service &io_service)
    : client_impl_(client_impl), io_service_(io_service) {}

Status ActorStateAccessor::AsyncGet(const ActorID &actor_id,
                                    const MultiItemCallback<ActorTableData> &callback) {
  io_service_.dispatch(
      boost::bind(&ActorStateAccessor::DoAsyncGet, this, actor_id, callback));
  return Status::OK();
}

void ActorStateAccessor::DoAsyncGet(const ActorID &actor_id,
                                    const MultiItemCallback<ActorTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ActorID &actor_id,
                            const std::vector<ActorTableData> &data) {
    callback(Status::OK(), data);
  };

  ActorTable &actor_table = client_impl_.actor_table();
  Status status = actor_table.Lookup(JobID::Nil(), actor_id, on_done);
  if (!status.ok()) {
    callback(status, {});
  }
}

Status ActorStateAccessor::AsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
                                         const StatusCallback &callback) {
  io_service_.dispatch(
      boost::bind(&ActorStateAccessor::DoAsyncRegister, this, data_ptr, callback));
  return Status::OK();
}

void ActorStateAccessor::DoAsyncRegister(const std::shared_ptr<ActorTableData> &data_ptr,
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
  Status status = actor_table.AppendAt(JobID::Nil(), actor_id, data_ptr, on_success,
                                       on_failure, /*log_length*/ 0);
  if (!status.ok() && callback != nullptr) {
    callback(status);
  }
}

Status ActorStateAccessor::AsyncUpdate(const ActorID &actor_id,
                                       const std::shared_ptr<ActorTableData> &data_ptr,
                                       const StatusCallback &callback) {
  io_service_.dispatch(boost::bind(&ActorStateAccessor::DoAsyncUpdate, this, actor_id,
                                   data_ptr, callback));
  return Status::OK();
}

void ActorStateAccessor::DoAsyncUpdate(const ActorID &actor_id,
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
  Status status = actor_table.AppendAt(JobID::Nil(), actor_id, data_ptr, on_success,
                                       on_failure, log_length);
  if (!status.ok() && callback != nullptr) {
    callback(status);
  }
}

Status ActorStateAccessor::AsyncSubscribe(
    const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  io_service_.dispatch(
      boost::bind(&ActorStateAccessor::DoAsyncSubscribe, this, subscribe, done));
  return Status::OK();
}

void ActorStateAccessor::DoAsyncSubscribe(
    const SubscribeCallback<ActorID, ActorTableData> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  auto on_subscribe = [subscribe](RedisGcsClient *client, const ActorID &actor_id,
                                  const std::vector<ActorTableData> &data) {
    if (!data.empty()) {
      // We only need the last entry, because it represents the latest state of
      // this actor.
      subscribe(actor_id, data.back());
    }
  };

  auto on_done = [done](RedisGcsClient *client) {
    if (done != nullptr) {
      done(Status::OK());
    }
  };

  ActorTable &actor_table = client_impl_.actor_table();
  Status status =
      actor_table.Subscribe(JobID::Nil(), ClientID::Nil(), on_subscribe, on_done);
  if (!status.ok() && done != nullptr) {
    done(status);
  }
}

}  // namespace gcs

}  // namespace ray

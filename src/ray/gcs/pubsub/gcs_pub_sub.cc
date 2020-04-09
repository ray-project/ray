// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gcs_pub_sub.h"
#include "ray/gcs/redis_context.h"

namespace ray {
namespace gcs {

template <typename ID, typename Data>
Status GcsPubSub<ID, Data>::Publish(const std::string &channel, const ID &id,
                                    const Data &data, const StatusCallback &done) {
  rpc::GcsMessage message;
  message.set_id(id.Binary());
  std::string data_str;
  data.SerializeToString(&data_str);
  message.set_data(data_str);

  auto on_done = [done](std::shared_ptr<CallbackReply> reply) {
    if (done) {
      done(Status::OK());
    }
  };

  return redis_client_->GetPrimaryContext()->PublishAsync(
      GenChannelPattern(channel, id), message.SerializeAsString(), on_done);
}

template <typename ID, typename Data>
Status GcsPubSub<ID, Data>::Subscribe(const std::string &channel, const ID &id,
                                      const Callback &subscribe,
                                      const StatusCallback &done) {
  return Subscribe(channel, boost::optional<ID>(id), subscribe, done);
}

template <typename ID, typename Data>
Status GcsPubSub<ID, Data>::SubscribeAll(const std::string &channel,
                                         const Callback &subscribe,
                                         const StatusCallback &done) {
  return Subscribe(channel, boost::none, subscribe, done);
}

template <typename ID, typename Data>
Status GcsPubSub<ID, Data>::Unsubscribe(const std::string &channel, const ID &id,
                                        const StatusCallback &done) {
  if (done) {
    unsubscribe_callbacks_[GenChannelPattern(channel, id)] = done;
  }
  return redis_client_->GetPrimaryContext()->PUnsubscribeAsync(
      GenChannelPattern(channel, id));
}

template <typename ID, typename Data>
Status GcsPubSub<ID, Data>::Subscribe(const std::string &channel,
                                      const boost::optional<ID> &id,
                                      const Callback &subscribe,
                                      const StatusCallback &done) {
  std::string pattern = GenChannelPattern(channel, id);
  auto callback = [this, pattern, subscribe](std::shared_ptr<CallbackReply> reply) {
    if (!reply->IsNil()) {
      if (reply->GetMessageType() == "punsubscribe") {
        if (unsubscribe_callbacks_.count(pattern)) {
          unsubscribe_callbacks_[pattern](Status::OK());
          unsubscribe_callbacks_.erase(pattern);
        }
        ray::gcs::RedisCallbackManager::instance().remove(
            subscribe_callback_index_[pattern]);
      } else {
        const auto reply_data = reply->ReadAsPubsubData();
        if (!reply_data.empty()) {
          rpc::GcsMessage message;
          message.ParseFromString(reply_data);
          Data data;
          data.ParseFromString(message.data());
          subscribe(ID::FromBinary(message.id()), data);
        }
      }
    }
  };

  int64_t out_callback_index;
  auto status = redis_client_->GetPrimaryContext()->PSubscribeAsync(pattern, callback,
                                                                    &out_callback_index);
  if (id) {
    subscribe_callback_index_[pattern] = out_callback_index;
  }
  if (done) {
    done(status);
  }
  return status;
}

template <typename ID, typename Data>
std::string GcsPubSub<ID, Data>::GenChannelPattern(const std::string &channel,
                                                   const boost::optional<ID> &id) {
  std::stringstream pattern;
  pattern << channel << ":";
  if (id) {
    pattern << id->Binary();
  } else {
    pattern << "*";
  }
  return pattern.str();
}

template class GcsPubSub<JobID, JobTableData>;
template class GcsPubSub<ActorID, ActorTableData>;
template class GcsPubSub<TaskID, TaskTableData>;
template class GcsPubSub<TaskID, TaskLeaseData>;
template class GcsPubSub<ObjectID, ObjectChange>;
template class GcsPubSub<ClientID, GcsNodeInfo>;
template class GcsPubSub<ClientID, ResourceChange>;
template class GcsPubSub<ClientID, HeartbeatTableData>;
template class GcsPubSub<ClientID, HeartbeatBatchTableData>;
template class GcsPubSub<WorkerID, WorkerFailureData>;

}  // namespace gcs
}  // namespace ray

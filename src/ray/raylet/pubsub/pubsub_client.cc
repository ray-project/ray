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

namespace ray {

// bool PubsubClient::ConnectIfNeeded(const Channel &channel, const EmptyCallback &connect_callback) {
//   // Needs asio
//   // If no connection yet:
//   //     Create a long polling connection.
//   //     Callback:
//   //         if status == false: release all objects subscribed.
//   //         Reply contains a list of objects. Obtain subscription callbacks and call the callback with -> post.
//   //         If disconnect = false: re-create a connection. 
//   //     connections.emplace(worker_id);
// }

// void PubsubClient::Subscribe(const Channel &channel, const ObjectID &object_id, const ObjectIdSubscriptionCallback &subscription_callback, const StatusCallback &callback) {
//   // Needs asio
//   // Send a RPC to the core worker with channel & object id.
//   // Callback: 
//   //     If status not okay, do nothing.
//   // channel_id -> object_id -> subscription_callback
// }

} // namespace ray

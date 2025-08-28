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

#pragma once

#include <memory>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/autoscaler.grpc.pb.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

// Most of our RPC templates, if not all, expect messages in the ray::rpc protobuf
// namespace.  Since the following two messages are defined under the rpc::events
// namespace, we treat them as if they were part of ray::rpc for compatibility.
using ray::rpc::events::AddEventsReply;
using ray::rpc::events::AddEventsRequest;

namespace ray {
namespace rpc {

#define MONITOR_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(MonitorGcsService,     \
                      HANDLER,               \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define OBJECT_INFO_SERVICE_RPC_HANDLER(HANDLER) \
  RPC_SERVICE_HANDLER(ObjectInfoGcsService,      \
                      HANDLER,                   \
                      RayConfig::instance().gcs_max_active_rpcs_per_handler())

#define GCS_RPC_SEND_REPLY(send_reply_callback, reply, status)        \
  reply->mutable_status()->set_code(static_cast<int>(status.code())); \
  reply->mutable_status()->set_message(status.message());             \
  send_reply_callback(ray::Status::OK(), nullptr, nullptr)

}  // namespace rpc
}  // namespace ray

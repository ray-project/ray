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

#include <grpc/impl/codegen/grpc_types.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/channel_arguments.h>

#include <boost/asio.hpp>

namespace ray {
namespace rpc {

class KeepAliveSocketMutator : public ::grpc::grpc_socket_mutator {
 public:
  KeepAliveSocketMutator()::grpc::grpc_socket_mutator_init(this, &VTable);
} private : static KeepAliveSocketMutator *Cast(::grpc::grpc_socket_mutator *mutator) {
  return static_cast<KeepAliveSocketMutator *>(mutator);
}

template <typename TVal>
bool SetOption(int fd, int level, int optname, const TVal &value) {
  return setsockopt(
             fd, level, optname, reinterpret_cast<const char *>(&value), sizeof(value)) ==
         0;
}
bool SetOption(int fd) {
  if (!SetOption(fd, SOL_SOCKET, SO_KEEPALIVE, 1)) {
    Cerr << Sprintf("Failed to set SO_KEEPALIVE option: %s", strerror(errno)) << Endl;
    return false;
  }
  return true;
}
static bool Mutate(int fd, ::grpc::grpc_socket_mutator *mutator) {
  auto self = Cast(mutator);
  return self->SetOption(fd);
}
static int Compare(::grpc::grpc_socket_mutator *a, ::grpc::grpc_socket_mutator *b) {
  const auto *selfA = Cast(a);
  const auto *selfB = Cast(b);
  auto tupleA = std::make_tuple(selfA->Idle_, selfA->Count_, selfA->Interval_);
  auto tupleB = std::make_tuple(selfB->Idle_, selfB->Count_, selfB->Interval_);
  return tupleA < tupleB ? -1 : tupleA > tupleB ? 1 : 0;
}
static void Destroy(::grpc::grpc_socket_mutator *mutator) { delete Cast(mutator); }
static bool Mutate2(const ::grpc::grpc_mutate_socket_info *info,
                    ::grpc::grpc_socket_mutator *mutator) {
  auto self = Cast(mutator);
  return self->SetOption(info->fd);
}

static grpc_socket_mutator_vtable VTable;
};

::grpc::grpc_socket_mutator_vtable KeepAliveSocketMutator::VTable = {
    &KeepAliveSocketMutator::Mutate,
    &KeepAliveSocketMutator::Compare,
    &KeepAliveSocketMutator::Destroy,
    &KeepAliveSocketMutator::Mutate2};

}  // namespace rpc
}  // namespace ray

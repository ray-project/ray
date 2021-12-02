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


#include "ray/rpc/grpc_chaos.h"

#include <string_view>
#include "absl/container/flat_hash_map.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {
namespace testing {
namespace {
class DelayManager {
 public:
  DelayManager() {
    auto delay_env = std::getenv("RAY_TESTING_RPC_DELAY");
    if(delay_env == nullptr) {
      return;
    }
    std::vector<std::string_view> items = absl::StrSplit(delay_env, ",");
    for(const auto& item : items) {
      std::vector<std::string_view> pair = absl::StrSplit(item, "@");
      if(pair.size() != 2 || (pair[1] != "client" && pair[1] != "server")) {
        RAY_LOG(ERROR) << "Error syntax: " << item << ", it has to be name=time@server or name=time@client";
        continue;
      }
      std::vector<std::string_view> delay = absl::StrSplit(pair[0], "=");
      size_t delay_ms = 0;

      if(delay.size() != 2 || !absl::SimpleAtoi(delay[1], &delay_ms)) {
        RAY_LOG(ERROR) << "Error syntax: " << item << ", it has to be name=time@server or name=time@client";
        continue;
      }
      RAY_LOG(WARNING) << "Inject grpc delay method = " << delay[0] << " for " << delay_ms << "ms" << " on " << pair[1] ;
      if(pair[1] == "client") {
        client_delay_[std::string{delay[0]}] = delay_ms;
      } else {
        server_delay_[std::string{delay[0]}] = delay_ms;
      }
    }
  }

  size_t GetServerMethodDelay(const std::string& name) {
    auto it = server_delay_.find(name);
    if(it == server_delay_.end()) {
      return 0;
    }
    return it->second;
  }

  size_t GetClientMethodDelay(const std::string& name) {
    auto it = client_delay_.find(name);
    if(it == client_delay_.end()) {
      return 0;
    }
    return it->second;
  }

 private:
  absl::flat_hash_map<std::string, size_t> server_delay_;
  absl::flat_hash_map<std::string, size_t> client_delay_;
};

static DelayManager _grpc_delay_maanger;

}

size_t server_delay(const std::string& name) {
  return _grpc_delay_maanger.GetServerMethodDelay(name);
}
size_t client_delay(const std::string& name) {
  return _grpc_delay_maanger.GetClientMethodDelay(name);
}

}
}
}

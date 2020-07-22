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

#include <functional>

#include "ray/gcs/redis_module/redismodule.h"

// NOTE(zongheng): this duplicated declaration serves as forward-declaration
// only.  The implementation is supposed to be linked in from credis.  In
// principle, we can expose a header from credis and simple include that header.
// This is left as future work.
//
// Concrete definitions from credis (from an example commit):
// https://github.com/ray-project/credis/blob/7eae7f2e58d16dfa1a95b5dfab02549f54b94e5d/src/member.cc#L41
// https://github.com/ray-project/credis/blob/7eae7f2e58d16dfa1a95b5dfab02549f54b94e5d/src/master.cc#L36

// Typical usage to make an existing redismodule command chain-compatible:
//
//   extern RedisChainModule module;
//   int MyCmd_RedisModuleCmd(...) {
//       return module.Mutate(..., NodeFunc, TailFunc);
//   }
//
// See, for instance, ChainTableAdd_RedisCommand in ray_redis_module.cc.
class RedisChainModule {
 public:
  // A function that runs on every node in the chain.  Type:
  //   (context, argv, argc, (can be nullptr) mutated_key_str) -> int
  //
  // (Advanced) The optional fourth arg can be used in the following way:
  //
  //     RedisModuleString* redis_key_str = nullptr;
  //     node_func(ctx, argv, argc, &redis_key_str);
  //     // "redis_key_str" now points to the RedisModuleString whose contents
  //     // is mutated by "node_func".
  //
  // If the fourth arg is passed, NodeFunc *must* fill in the key being mutated.
  // It is okay for this NodeFunc to call "RM_FreeString(mutated_key_str)" after
  // assigning the fourth arg, since that call presumably only decrements a ref
  // count.
  using NodeFunc = std::function<int(RedisModuleCtx *, RedisModuleString **, int,
                                     RedisModuleString **)>;

  // A function that (1) runs only after all NodeFunc's have run, and (2) runs
  // once on the tail.  A typical usage is to publish a write.
  using TailFunc = std::function<int(RedisModuleCtx *, RedisModuleString **, int)>;

  // TODO(zongheng): document the RM_Reply semantics.

  // Runs "node_func" on every node in the chain; after the tail node has run it
  // too, finalizes the mutation by running "tail_func".
  //
  // If node_func() returns non-zero, it is treated as an error and the entire
  // update will terminate early, without running subsequent node_func() and the
  // final tail_func().
  //
  // TODO(zongheng): currently only supports 1-node chain.
  int ChainReplicate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                     NodeFunc node_func, TailFunc tail_func);
};

#ifndef RAY_CHAIN_MODULE_H_
#define RAY_CHAIN_MODULE_H_

#include <functional>

#include "redismodule.h"

// NOTE(zongheng): this duplicated declaration serves as forward-declaration
// only.  The implementation is supposed to be linked in from credis.  In
// principle, we can expose a header from credis and simple include that header.
// This is left as future work.

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
  // If the fourth arg is passed, NodeFunc must fill in the key being mutated.
  // It is okay for this NodeFunc to call "RM_FreeString(mutated_key_str)" after
  // assigning the fourth arg, since that call presumably only decrements a ref
  // count.
  using NodeFunc = std::function<
      int(RedisModuleCtx *, RedisModuleString **, int, RedisModuleString **)>;

  // A function that (1) runs only after all NodeFunc's have run, and (2) runs
  // once on the tail.  A typical usage is to publish a write.
  using TailFunc =
      std::function<int(RedisModuleCtx *, RedisModuleString **, int)>;

  // TODO(zongheng): document the RM_Reply semantics.

  // Runs "node_func" on every node in the chain; after the tail node has run it
  // too, finalizes the mutation by running "tail_func".
  // TODO(zongheng): currently only supports 1-node chain.
  int Mutate(RedisModuleCtx *ctx,
             RedisModuleString **argv,
             int argc,
             NodeFunc node_func,
             TailFunc tail_func);
};

#endif  // RAY_CHAIN_MODULE_H_

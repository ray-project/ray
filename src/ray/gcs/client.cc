#include "ray/gcs/client.h"

#include "ray/gcs/redis_context.h"

static void GetRedisShards(redisContext *context, std::vector<std::string> &addresses,
                           std::vector<int> &ports) {
  // Get the total number of Redis shards in the system.
  int num_attempts = 0;
  redisReply *reply = nullptr;
  while (num_attempts < RayConfig::instance().redis_db_connect_retries()) {
    // Try to read the number of Redis shards from the primary shard. If the
    // entry is present, exit.
    reply = reinterpret_cast<redisReply *>(redisCommand(context, "GET NumRedisShards"));
    if (reply->type != REDIS_REPLY_NIL) {
      break;
    }

    // Sleep for a little, and try again if the entry isn't there yet. */
    freeReplyObject(reply);
    usleep(RayConfig::instance().redis_db_connect_wait_milliseconds() * 1000);
    num_attempts++;
  }
  RAY_CHECK(num_attempts < RayConfig::instance().redis_db_connect_retries())
      << "No entry found for NumRedisShards";
  RAY_CHECK(reply->type == REDIS_REPLY_STRING) << "Expected string, found Redis type "
                                               << reply->type << " for NumRedisShards";
  int num_redis_shards = atoi(reply->str);
  RAY_CHECK(num_redis_shards >= 1) << "Expected at least one Redis shard, "
                                   << "found " << num_redis_shards;
  freeReplyObject(reply);

  // Get the addresses of all of the Redis shards.
  num_attempts = 0;
  while (num_attempts < RayConfig::instance().redis_db_connect_retries()) {
    // Try to read the Redis shard locations from the primary shard. If we find
    // that all of them are present, exit.
    reply =
        reinterpret_cast<redisReply *>(redisCommand(context, "LRANGE RedisShards 0 -1"));
    if (static_cast<int>(reply->elements) == num_redis_shards) {
      break;
    }

    // Sleep for a little, and try again if not all Redis shard addresses have
    // been added yet.
    freeReplyObject(reply);
    usleep(RayConfig::instance().redis_db_connect_wait_milliseconds() * 1000);
    num_attempts++;
  }
  RAY_CHECK(num_attempts < RayConfig::instance().redis_db_connect_retries())
      << "Expected " << num_redis_shards << " Redis shard addresses, found "
      << reply->elements;

  // Parse the Redis shard addresses.
  for (size_t i = 0; i < reply->elements; ++i) {
    // Parse the shard addresses and ports.
    RAY_CHECK(reply->element[i]->type == REDIS_REPLY_STRING);
    std::string addr;
    std::stringstream ss(reply->element[i]->str);
    getline(ss, addr, ':');
    addresses.push_back(addr);
    int port;
    ss >> port;
    ports.push_back(port);
  }
  freeReplyObject(reply);
}

namespace ray {

namespace gcs {

AsyncGcsClient::AsyncGcsClient(const std::string &address, int port,
                               const ClientID &client_id, CommandType command_type,
                               bool is_test_client = false) {
  primary_context_ = std::make_shared<RedisContext>();

  RAY_CHECK_OK(primary_context_->Connect(address, port, /*sharding=*/true));

  if (!is_test_client) {
    // Moving sharding into constructor defaultly means that sharding = true.
    // This design decision may worth a look.
    std::vector<std::string> addresses;
    std::vector<int> ports;
    GetRedisShards(primary_context_->sync_context(), addresses, ports);
    if (addresses.size() == 0 || ports.size() == 0) {
      addresses.push_back(address);
      ports.push_back(port);
    }

    // Populate shard_contexts.
    for (size_t i = 0; i < addresses.size(); ++i) {
      shard_contexts_.push_back(std::make_shared<RedisContext>());
    }

    RAY_CHECK(shard_contexts_.size() == addresses.size());
    for (size_t i = 0; i < addresses.size(); ++i) {
      RAY_CHECK_OK(
          shard_contexts_[i]->Connect(addresses[i], ports[i], /*sharding=*/true));
    }
  } else {
    shard_contexts_.push_back(std::make_shared<RedisContext>());
    RAY_CHECK_OK(shard_contexts_[0]->Connect(address, port, /*sharding=*/true));
  }

  client_table_.reset(new ClientTable({primary_context_}, this, client_id));
  error_table_.reset(new ErrorTable({primary_context_}, this));
  driver_table_.reset(new DriverTable({primary_context_}, this));
  // Tables below would be sharded.
  object_table_.reset(new ObjectTable(shard_contexts_, this, command_type));
  actor_table_.reset(new ActorTable(shard_contexts_, this));
  task_table_.reset(new TaskTable(shard_contexts_, this, command_type));
  raylet_task_table_.reset(new raylet::TaskTable(shard_contexts_, this, command_type));
  task_reconstruction_log_.reset(new TaskReconstructionLog(shard_contexts_, this));
  task_lease_table_.reset(new TaskLeaseTable(shard_contexts_, this));
  heartbeat_table_.reset(new HeartbeatTable(shard_contexts_, this));
  profile_table_.reset(new ProfileTable(shard_contexts_, this));
  command_type_ = command_type;

  // TODO(swang): Call the client table's Connect() method here. To do this,
  // we need to make sure that we are attached to an event loop first. This
  // currently isn't possible because the aeEventLoop, which we use for
  // testing, requires us to connect to Redis first.
}

#if RAY_USE_NEW_GCS
// Use of kChain currently only applies to Table::Add which affects only the
// task table, and when RAY_USE_NEW_GCS is set at compile time.
AsyncGcsClient::AsyncGcsClient(const std::string &address, int port,
                               const ClientID &client_id, bool is_test_client = false)
    : AsyncGcsClient(address, port, client_id, CommandType::kChain, is_test_client) {}
#else
AsyncGcsClient::AsyncGcsClient(const std::string &address, int port,
                               const ClientID &client_id, bool is_test_client = false)
    : AsyncGcsClient(address, port, client_id, CommandType::kRegular, is_test_client) {}
#endif  // RAY_USE_NEW_GCS

AsyncGcsClient::AsyncGcsClient(const std::string &address, int port,
                               CommandType command_type)
    : AsyncGcsClient(address, port, ClientID::from_random(), command_type) {}

AsyncGcsClient::AsyncGcsClient(const std::string &address, int port,
                               CommandType command_type, bool is_test_client)
    : AsyncGcsClient(address, port, ClientID::from_random(), command_type,
                     is_test_client) {}

AsyncGcsClient::AsyncGcsClient(const std::string &address, int port)
    : AsyncGcsClient(address, port, ClientID::from_random()) {}

AsyncGcsClient::AsyncGcsClient(const std::string &address, int port, bool is_test_client)
    : AsyncGcsClient(address, port, ClientID::from_random(), is_test_client) {}

Status Attach(plasma::EventLoop &event_loop) {
  // TODO(pcm): Implement this via
  // context()->AttachToEventLoop(event loop)
  return Status::OK();
}

Status AsyncGcsClient::Attach(boost::asio::io_service &io_service) {
  // Take care of sharding contexts.
  RAY_CHECK(shard_asio_async_clients_.empty()) << "Attach shall be called only once";
  for (std::shared_ptr<RedisContext> context : shard_contexts_) {
    shard_asio_async_clients_.emplace_back(
        new RedisAsioClient(io_service, context->async_context()));
    shard_asio_subscribe_clients_.emplace_back(
        new RedisAsioClient(io_service, context->subscribe_context()));
  }
  asio_async_auxiliary_client_.reset(
      new RedisAsioClient(io_service, primary_context_->async_context()));
  asio_subscribe_auxiliary_client_.reset(
      new RedisAsioClient(io_service, primary_context_->subscribe_context()));
  return Status::OK();
}

ObjectTable &AsyncGcsClient::object_table() { return *object_table_; }

TaskTable &AsyncGcsClient::task_table() { return *task_table_; }

raylet::TaskTable &AsyncGcsClient::raylet_task_table() { return *raylet_task_table_; }

ActorTable &AsyncGcsClient::actor_table() { return *actor_table_; }

TaskReconstructionLog &AsyncGcsClient::task_reconstruction_log() {
  return *task_reconstruction_log_;
}

TaskLeaseTable &AsyncGcsClient::task_lease_table() { return *task_lease_table_; }

ClientTable &AsyncGcsClient::client_table() { return *client_table_; }

FunctionTable &AsyncGcsClient::function_table() { return *function_table_; }

ClassTable &AsyncGcsClient::class_table() { return *class_table_; }

HeartbeatTable &AsyncGcsClient::heartbeat_table() { return *heartbeat_table_; }

ErrorTable &AsyncGcsClient::error_table() { return *error_table_; }

DriverTable &AsyncGcsClient::driver_table() { return *driver_table_; }

ProfileTable &AsyncGcsClient::profile_table() { return *profile_table_; }

}  // namespace gcs

}  // namespace ray

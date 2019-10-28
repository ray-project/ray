#ifndef RAY_STREAMING_RAY_TEST_COMMON_H
#define RAY_STREAMING_RAY_TEST_COMMON_H

#include <unistd.h>
#include <thread>
#include "gtest/gtest.h"

#include "streaming.h"
#include "streaming_message.h"
#include "streaming_message_bundle.h"
#include "streaming_reader.h"
#include "streaming_ring_buffer.h"
#include "streaming_writer.h"

#include "ray/gcs/redis_gcs_client.h"
#include "ray/raylet/raylet_client.h"

namespace ray {
namespace streaming {

static void start_redis(const std::string &redis_executable,
                        const std::string &ray_redis_module_file) {
  std::string primary_redis_start_command = redis_executable + " --port 6379" +
                                            " --loadmodule " + ray_redis_module_file +
                                            " --loglevel warning &";
  std::string shard_redis_start_command = redis_executable + " --port 6380" +
                                          " --loadmodule " + ray_redis_module_file +
                                          " --loglevel warning &";
  RAY_LOG(INFO) << "Redis primary start command: " << primary_redis_start_command;
  RAY_LOG(INFO) << "Redis shard start command: " << shard_redis_start_command;
  system(primary_redis_start_command.c_str());
  system(shard_redis_start_command.c_str());
  usleep(200 * 1000);
}

static void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  freeReplyObject(redisCommand(context, "LPUSH RedisShards 127.0.0.1:6380"));
  redisFree(context);
}

ray::ObjectID GenerateRandomPipeID() { return ObjectID::FromRandom(); }

class StreamingRayPipeTest : public ::testing::Test {
 public:
  StreamingRayPipeTest() { job_id_ = JobID::FromInt(10); }

  ~StreamingRayPipeTest() {
    StopRaylet(raylet_socket_name_1_);
    StopRaylet(raylet_socket_name_2_);
    StopStore(store_socket_name_1_);
    StopStore(store_socket_name_2_);
  }

  std::string StartStore() {
    std::string store_socket_name = "/tmp/store" + GenerateRandomPipeID().Hex();
    std::string store_pid = store_socket_name + ".pid";
    std::string plasma_command = store_executable + " -m 500000000 -s " +
                                 store_socket_name +
                                 // " 1> /dev/null 2> /dev/null & echo $! > " +
                                 " & echo $! > " + store_pid;
    RAY_LOG(INFO) << plasma_command;
    RAY_CHECK(system(plasma_command.c_str()) == 0);
    usleep(200 * 1000);
    return store_socket_name;
  }

  void StopStore(std::string store_socket_name) {
    std::string store_pid = store_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + store_pid + "`";
    RAY_LOG(INFO) << kill_9;
    system(kill_9.c_str());
    system(("rm -rf " + store_socket_name).c_str());
    system(("rm -rf " + store_socket_name + ".pid").c_str());
  }

  std::string StartRaylet(std::string store_socket_name, std::string node_ip_address,
                          std::string redis_address, std::string resource) {
    std::string raylet_socket_name = "/tmp/raylet" + GenerateRandomPipeID().Hex();
    std::string ray_start_cmd = raylet_executable;
    ray_start_cmd.append(" --raylet_socket_name=" + raylet_socket_name)
        .append(" --store_socket_name=" + store_socket_name)
        .append(" --object_manager_port=0 --node_manager_port=0")  // object manager port
                                                                   // & node manager port
        .append(" --node_ip_address=" + node_ip_address)
        .append(" --redis_address=" + redis_address)
        .append(" --redis_port=6379")                 // redis port
        .append(" --num_initial_workers=0")           // work num
        .append(" --maximum_startup_concurrency=10")  // max worker
        .append(" --static_resource_list=" + resource)
        .append(" --config_list=\"\"")
        .append(" --python_worker_command=\"NoneCmd\"")
        .append(" & echo $! > " + raylet_socket_name + ".pid");

    RAY_LOG(INFO) << "Ray Start command: " << ray_start_cmd;
    RAY_CHECK(system(ray_start_cmd.c_str()) == 0);
    usleep(200 * 1000);
    return raylet_socket_name;
  }

  void StopRaylet(std::string raylet_socket_name) {
    std::string raylet_pid = raylet_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + raylet_pid + "`";
    RAY_LOG(INFO) << kill_9;
    system(kill_9.c_str());
    system(("rm -rf " + raylet_socket_name).c_str());
    system(("rm -rf " + raylet_socket_name + ".pid").c_str());
  }

  void SetUp() {
    // start plasma store.
    store_socket_name_1_ = StartStore();
    store_socket_name_2_ = StartStore();

    // start raylet on head node
    raylet_socket_name_1_ = StartRaylet(store_socket_name_1_, "127.0.0.1", "127.0.0.1",
                                        "\"CPU,4.0,resA,100\"");

    // start raylet on non-head node
    raylet_socket_name_2_ = StartRaylet(store_socket_name_2_, "127.0.0.1", "127.0.0.1",
                                        "\"CPU,4.0,resB,100\"");
    flushall_redis();
  }

  void TearDown() {
    StopRaylet(raylet_socket_name_1_);
    StopRaylet(raylet_socket_name_2_);
    StopStore(store_socket_name_1_);
    StopStore(store_socket_name_2_);
  }

  uint8_t *get_test_data(int len) {
    auto *data = new uint8_t[len];
    for (int i = 0; i < len; ++i) {
      data[i] = static_cast<uint8_t>(i % 256);
    }
    return data;
  }

  bool check_test_data(uint8_t *data, int len) {
    for (int i = 0; i < len; ++i) {
      if (data[i] != i % 256) return false;
    }
    return true;
  }

  bool check_test_reader_msg(std::shared_ptr<StreamingReaderBundle> &msg) {
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(msg->data);
    std::list<StreamingMessagePtr> msg_list;
    bundle_ptr->GetMessageList(msg_list);
    for (auto &msg_ptr : msg_list) {
      if (!check_test_data(msg_ptr->RawData(), msg_ptr->GetDataSize())) {
        return false;
      }
    }
    return true;
  }

 public:
  static std::string store_executable;
  static std::string raylet_executable;

 protected:
  boost::asio::io_service main_service_1_;
  boost::asio::io_service main_service_2_;

  std::string raylet_socket_name_1_;
  std::string raylet_socket_name_2_;
  std::string store_socket_name_1_;
  std::string store_socket_name_2_;

  JobID job_id_;
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_RAY_TEST_COMMON_H

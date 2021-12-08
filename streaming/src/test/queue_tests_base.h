#pragma once

#include "hiredis/hiredis.h"
#include "ray/common/common_protocol.h"
#include "ray/common/test_util.h"
#include "ray/util/filesystem.h"

namespace ray {
namespace streaming {

using namespace ray::core;

ray::ObjectID RandomObjectID() { return ObjectID::FromRandom(); }

static void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  freeReplyObject(redisCommand(context, "LPUSH RedisShards 127.0.0.1:6380"));
  redisFree(context);
}
/// Base class for real-world tests with streaming queue
class StreamingQueueTestBase : public ::testing::TestWithParam<uint64_t> {
 public:
  StreamingQueueTestBase(int num_nodes, int port)
      : gcs_options_("127.0.0.1", 6379, ""), node_manager_port_(port) {
    TestSetupUtil::StartUpRedisServers(std::vector<int>{6379, 6380});

    // flush redis first.
    flushall_redis();

    RAY_CHECK(num_nodes >= 0);
    if (num_nodes > 0) {
      raylet_socket_names_.resize(num_nodes);
      raylet_store_socket_names_.resize(num_nodes);
    }

    // start gcs server
    gcs_server_socket_name_ = TestSetupUtil::StartGcsServer("127.0.0.1");

    // start raylet on each node. Assign each node with different resources so that
    // a task can be scheduled to the desired node.
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] =
          TestSetupUtil::StartRaylet("127.0.0.1", node_manager_port_ + i, "127.0.0.1",
                                     "\"CPU,4.0,resource" + std::to_string(i) + ",10\"",
                                     &raylet_store_socket_names_[i]);
    }
  }

  ~StreamingQueueTestBase() {
    STREAMING_LOG(INFO) << "Stop raylet store and actors";
    for (const auto &raylet_socket_name : raylet_socket_names_) {
      TestSetupUtil::StopRaylet(raylet_socket_name);
    }

    TestSetupUtil::StopGcsServer(gcs_server_socket_name_);
    TestSetupUtil::ShutDownRedisServers();
  }

  JobID NextJobId() const {
    static uint32_t job_counter = 1;
    return JobID::FromInt(job_counter++);
  }

  void InitWorker(ActorID &self_actor_id, ActorID &peer_actor_id,
                  const queue::protobuf::StreamingQueueTestRole role,
                  const std::vector<ObjectID> &queue_ids,
                  const std::vector<ObjectID> &rescale_queue_ids, std::string suite_name,
                  std::string test_name, uint64_t param) {
    auto &driver = CoreWorkerProcess::GetCoreWorker();
    std::string forked_serialized_str;
    ObjectID actor_handle_id;
    Status st = driver.SerializeActorHandle(peer_actor_id, &forked_serialized_str,
                                            &actor_handle_id);
    STREAMING_CHECK(st.ok());
    STREAMING_LOG(INFO) << "forked_serialized_str: " << forked_serialized_str;
    TestInitMessage msg(role, self_actor_id, peer_actor_id, forked_serialized_str,
                        queue_ids, rescale_queue_ids, suite_name, test_name, param);

    std::vector<std::unique_ptr<TaskArg>> args;
    args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
        msg.ToBytes(), nullptr, std::vector<rpc::ObjectReference>(), true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options{"", 0, resources};
    RayFunction func{ray::Language::PYTHON,
                     ray::FunctionDescriptorBuilder::BuildPython("", "", "init", "")};

    RAY_UNUSED(driver.SubmitActorTask(self_actor_id, func, args, options));
  }

  void SubmitTestToActor(ActorID &actor_id, const std::string test) {
    auto &driver = CoreWorkerProcess::GetCoreWorker();
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);
    std::vector<std::unique_ptr<TaskArg>> args;
    args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
        buffer, nullptr, std::vector<rpc::ObjectReference>(), true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options("", 0, resources);
    RayFunction func{ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "", test, "execute_test", "")};

    RAY_UNUSED(driver.SubmitActorTask(actor_id, func, args, options));
  }

  bool CheckCurTest(ActorID &actor_id, const std::string test_name) {
    auto &driver = CoreWorkerProcess::GetCoreWorker();
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);
    std::vector<std::unique_ptr<TaskArg>> args;
    args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
        buffer, nullptr, std::vector<rpc::ObjectReference>(), true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options{"", 1, resources};
    RayFunction func{ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "", "", "check_current_test_status", "")};

    auto return_refs = driver.SubmitActorTask(actor_id, func, args, options);
    auto return_ids = ObjectRefsToIds(return_refs.value());

    std::vector<bool> wait_results;
    std::vector<std::shared_ptr<RayObject>> results;
    Status wait_st = driver.Wait(return_ids, 1, 5 * 1000, &wait_results, true);
    if (!wait_st.ok()) {
      STREAMING_LOG(ERROR) << "Wait fail.";
      return false;
    }
    STREAMING_CHECK(wait_results.size() >= 1);
    if (!wait_results[0]) {
      STREAMING_LOG(WARNING) << "Wait direct call fail.";
      return false;
    }

    Status get_st = driver.Get(return_ids, -1, &results);
    if (!get_st.ok()) {
      STREAMING_LOG(ERROR) << "Get fail.";
      return false;
    }
    STREAMING_CHECK(results.size() >= 1);
    if (results[0]->IsException()) {
      STREAMING_LOG(INFO) << "peer actor may has exceptions.";
      return false;
    }
    STREAMING_CHECK(results[0]->HasData());
    STREAMING_LOG(DEBUG) << "SendForResult result[0] DataSize: " << results[0]->GetSize();

    const std::shared_ptr<ray::Buffer> result_buffer = results[0]->GetData();
    std::shared_ptr<LocalMemoryBuffer> return_buffer =
        std::make_shared<LocalMemoryBuffer>(result_buffer->Data(), result_buffer->Size(),
                                            true);

    uint8_t *bytes = result_buffer->Data();
    uint8_t *p_cur = bytes;
    uint32_t *magic_num = (uint32_t *)p_cur;
    STREAMING_CHECK(*magic_num == Message::MagicNum);

    p_cur += sizeof(Message::MagicNum);
    queue::protobuf::StreamingQueueMessageType *type =
        (queue::protobuf::StreamingQueueMessageType *)p_cur;
    STREAMING_CHECK(*type == queue::protobuf::StreamingQueueMessageType::
                                 StreamingQueueTestCheckStatusRspMsgType);
    std::shared_ptr<TestCheckStatusRspMsg> message =
        TestCheckStatusRspMsg::FromBytes(bytes);
    STREAMING_CHECK(message->TestName() == test_name);
    return message->Status();
  }

  ActorID CreateActorHelper(const std::unordered_map<std::string, double> &resources,
                            bool is_direct_call, int64_t max_restarts) {
    std::unique_ptr<ActorHandle> actor_handle;

    // Test creating actor.
    uint8_t array[] = {1, 2, 3};
    auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));

    RayFunction func{ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "", "", "actor creation task", "")};
    std::vector<std::unique_ptr<TaskArg>> args;
    args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
        buffer, nullptr, std::vector<rpc::ObjectReference>())));

    std::string name = "";
    std::string ray_namespace = "";
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_default_scheduling_strategy();
    ActorCreationOptions actor_options{
        max_restarts,
        /*max_task_retries=*/0,
        /*max_concurrency=*/1,  resources, resources,     {},
        /*is_detached=*/false,  name,      ray_namespace, /*is_asyncio=*/false,
        scheduling_strategy};
    // Create an actor.
    ActorID actor_id;
    RAY_CHECK_OK(CoreWorkerProcess::GetCoreWorker().CreateActor(
        func, args, actor_options, /*extension_data*/ "", &actor_id));
    return actor_id;
  }

  void SubmitTest(uint32_t queue_num, std::string suite_name, std::string test_name,
                  uint64_t timeout_ms) {
    std::vector<ray::ObjectID> queue_id_vec;
    std::vector<ray::ObjectID> rescale_queue_id_vec;
    for (uint32_t i = 0; i < queue_num; ++i) {
      ObjectID queue_id = ray::ObjectID::FromRandom();
      queue_id_vec.emplace_back(queue_id);
    }

    // One scale id
    ObjectID rescale_queue_id = ray::ObjectID::FromRandom();
    rescale_queue_id_vec.emplace_back(rescale_queue_id);

    std::vector<uint64_t> channel_seq_id_vec(queue_num, 0);

    for (size_t i = 0; i < queue_id_vec.size(); ++i) {
      STREAMING_LOG(INFO) << " qid hex => " << queue_id_vec[i].Hex();
    }
    for (auto &qid : rescale_queue_id_vec) {
      STREAMING_LOG(INFO) << " rescale qid hex => " << qid.Hex();
    }
    STREAMING_LOG(INFO) << "Sub process: writer.";

    // You must keep it same with `src/ray/core_worker/core_worker.h:CoreWorkerOptions`
    CoreWorkerOptions options;
    options.worker_type = WorkerType::DRIVER;
    options.language = Language::PYTHON;
    options.store_socket = raylet_store_socket_names_[0];
    options.raylet_socket = raylet_socket_names_[0];
    options.job_id = NextJobId();
    options.gcs_options = gcs_options_;
    options.enable_logging = true;
    options.install_failure_signal_handler = true;
    options.node_ip_address = "127.0.0.1";
    options.node_manager_port = node_manager_port_;
    options.raylet_ip_address = "127.0.0.1";
    options.driver_name = "queue_tests";
    options.num_workers = 1;
    options.metrics_agent_port = -1;
    InitShutdownRAII core_worker_raii(CoreWorkerProcess::Initialize,
                                      CoreWorkerProcess::Shutdown, options);

    // Create writer and reader actors
    std::unordered_map<std::string, double> resources;
    auto actor_id_writer = CreateActorHelper(resources, true, 0);
    auto actor_id_reader = CreateActorHelper(resources, true, 0);

    InitWorker(actor_id_writer, actor_id_reader,
               queue::protobuf::StreamingQueueTestRole::WRITER, queue_id_vec,
               rescale_queue_id_vec, suite_name, test_name, GetParam());
    InitWorker(actor_id_reader, actor_id_writer,
               queue::protobuf::StreamingQueueTestRole::READER, queue_id_vec,
               rescale_queue_id_vec, suite_name, test_name, GetParam());

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    SubmitTestToActor(actor_id_writer, test_name);
    SubmitTestToActor(actor_id_reader, test_name);

    uint64_t slept_time_ms = 0;
    while (slept_time_ms < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5 * 1000));
      STREAMING_LOG(INFO) << "Check test status.";
      if (CheckCurTest(actor_id_writer, test_name) &&
          CheckCurTest(actor_id_reader, test_name)) {
        STREAMING_LOG(INFO) << "Test Success, Exit.";
        return;
      }
      slept_time_ms += 5 * 1000;
    }

    EXPECT_TRUE(false);
    STREAMING_LOG(INFO) << "Test Timeout, Exit.";
  }

  void SetUp() {}

  void TearDown() {}

 protected:
  std::vector<std::string> raylet_socket_names_;
  std::vector<std::string> raylet_store_socket_names_;
  gcs::GcsClientOptions gcs_options_;
  int node_manager_port_;
  std::string gcs_server_socket_name_;
};

}  // namespace streaming
}  // namespace ray

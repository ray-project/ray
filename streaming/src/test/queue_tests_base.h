#include "ray/util/filesystem.h"

namespace ray {
namespace streaming {

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
  StreamingQueueTestBase(int num_nodes, std::string raylet_exe, std::string store_exe,
                         int port, std::string actor_exe, std::string gcs_server_exe)
      : gcs_options_("127.0.0.1", 6379, ""),
        raylet_executable_(raylet_exe),
        gcs_server_executable_(gcs_server_exe),
        store_executable_(store_exe),
        actor_executable_(actor_exe),
        node_manager_port_(port) {
#ifdef _WIN32
    RAY_CHECK(false) << "port system() calls to Windows before running this test";
#endif

    // flush redis first.
    flushall_redis();

    RAY_CHECK(num_nodes >= 0);
    if (num_nodes > 0) {
      raylet_socket_names_.resize(num_nodes);
      raylet_store_socket_names_.resize(num_nodes);
    }

    // start plasma store.
    for (auto &store_socket : raylet_store_socket_names_) {
      store_socket = StartStore();
    }

    // start gcs server
    gcs_server_pid_ = StartGcsServer("127.0.0.1");

    // start raylet on each node. Assign each node with different resources so that
    // a task can be scheduled to the desired node.
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] =
          StartRaylet(raylet_store_socket_names_[i], "127.0.0.1", node_manager_port_ + i,
                      "127.0.0.1", "\"CPU,4.0,resource" + std::to_string(i) + ",10\"");
    }
  }

  ~StreamingQueueTestBase() {
    STREAMING_LOG(INFO) << "Stop raylet store and actors";
    for (const auto &raylet_socket : raylet_socket_names_) {
      StopRaylet(raylet_socket);
    }

    for (const auto &store_socket : raylet_store_socket_names_) {
      StopStore(store_socket);
    }

    StopGcsServer(gcs_server_pid_);
  }

  JobID NextJobId() const {
    static uint32_t job_counter = 1;
    return JobID::FromInt(job_counter++);
  }

  std::string StartStore() {
    std::string store_socket_name =
        ray::JoinPaths(ray::GetUserTempDir(), "store" + RandomObjectID().Hex());
    std::string store_pid = store_socket_name + ".pid";
    std::string plasma_command = store_executable_ + " -m 10000000 -s " +
                                 store_socket_name +
                                 " 1> /dev/null 2> /dev/null & echo $! > " + store_pid;
    RAY_LOG(DEBUG) << plasma_command;
    RAY_CHECK(system(plasma_command.c_str()) == 0);
    usleep(200 * 1000);
    return store_socket_name;
  }

  void StopStore(std::string store_socket_name) {
    std::string store_pid = store_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + store_pid + "`";
    RAY_LOG(DEBUG) << kill_9;
    ASSERT_EQ(system(kill_9.c_str()), 0);
    ASSERT_EQ(system(("rm -rf " + store_socket_name).c_str()), 0);
    ASSERT_EQ(system(("rm -rf " + store_socket_name + ".pid").c_str()), 0);
  }

  std::string StartGcsServer(std::string redis_address) {
    std::string gcs_server_socket_name = ray::JoinPaths(
        ray::GetUserTempDir(), "gcs_server" + ObjectID::FromRandom().Hex());
    std::string ray_start_cmd = gcs_server_executable_;
    ray_start_cmd.append(" --redis_address=" + redis_address)
        .append(" --redis_port=6379")
        .append(" --config_list=initial_reconstruction_timeout_milliseconds,2000")
        .append(" & echo $! > " + gcs_server_socket_name + ".pid");

    RAY_LOG(INFO) << "Start gcs server command: " << ray_start_cmd;
    RAY_CHECK(system(ray_start_cmd.c_str()) == 0);
    usleep(200 * 1000);
    RAY_LOG(INFO) << "Finished start gcs server.";
    return gcs_server_socket_name;
  }

  void StopGcsServer(std::string gcs_server_socket_name) {
    std::string gcs_server_pid = gcs_server_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + gcs_server_pid + "`";
    RAY_LOG(DEBUG) << kill_9;
    ASSERT_TRUE(system(kill_9.c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + gcs_server_socket_name).c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + gcs_server_socket_name + ".pid").c_str()) == 0);
  }

  std::string StartRaylet(std::string store_socket_name, std::string node_ip_address,
                          int port, std::string redis_address, std::string resource) {
    std::string raylet_socket_name =
        ray::JoinPaths(ray::GetUserTempDir(), "raylet" + RandomObjectID().Hex());
    std::string ray_start_cmd = raylet_executable_;
    ray_start_cmd.append(" --raylet_socket_name=" + raylet_socket_name)
        .append(" --store_socket_name=" + store_socket_name)
        .append(" --object_manager_port=0 --node_manager_port=" + std::to_string(port))
        .append(" --node_ip_address=" + node_ip_address)
        .append(" --redis_address=" + redis_address)
        .append(" --redis_port=6379")
        .append(" --num_initial_workers=1")
        .append(" --maximum_startup_concurrency=10")
        .append(" --static_resource_list=" + resource)
        .append(" --python_worker_command=\"" + actor_executable_ + " " +
                store_socket_name + " " + raylet_socket_name + " " +
                std::to_string(port) + "\"")
        .append(" --config_list=initial_reconstruction_timeout_milliseconds,2000")
        .append(" & echo $! > " + raylet_socket_name + ".pid");

    RAY_LOG(DEBUG) << "Ray Start command: " << ray_start_cmd;
    RAY_CHECK(system(ray_start_cmd.c_str()) == 0);
    usleep(200 * 1000);
    return raylet_socket_name;
  }

  void StopRaylet(std::string raylet_socket_name) {
    std::string raylet_pid = raylet_socket_name + ".pid";
    std::string kill_9 = "kill -9 `cat " + raylet_pid + "`";
    RAY_LOG(DEBUG) << kill_9;
    ASSERT_TRUE(system(kill_9.c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + raylet_socket_name).c_str()) == 0);
    ASSERT_TRUE(system(("rm -rf " + raylet_socket_name + ".pid").c_str()) == 0);
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

    std::vector<TaskArg> args;
    args.emplace_back(TaskArg::PassByValue(std::make_shared<RayObject>(
        msg.ToBytes(), nullptr, std::vector<ObjectID>(), true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options{0, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func{ray::Language::PYTHON,
                     ray::FunctionDescriptorBuilder::BuildPython("", "", "init", "")};

    RAY_CHECK_OK(driver.SubmitActorTask(self_actor_id, func, args, options, &return_ids));
  }

  void SubmitTestToActor(ActorID &actor_id, const std::string test) {
    auto &driver = CoreWorkerProcess::GetCoreWorker();
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);
    std::vector<TaskArg> args;
    args.emplace_back(TaskArg::PassByValue(
        std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>(), true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options{0, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func{ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "", test, "execute_test", "")};

    RAY_CHECK_OK(driver.SubmitActorTask(actor_id, func, args, options, &return_ids));
  }

  bool CheckCurTest(ActorID &actor_id, const std::string test_name) {
    auto &driver = CoreWorkerProcess::GetCoreWorker();
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);
    std::vector<TaskArg> args;
    args.emplace_back(TaskArg::PassByValue(
        std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>(), true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options{1, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func{ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "", "", "check_current_test_status", "")};

    RAY_CHECK_OK(driver.SubmitActorTask(actor_id, func, args, options, &return_ids));

    std::vector<bool> wait_results;
    std::vector<std::shared_ptr<RayObject>> results;
    Status wait_st = driver.Wait(return_ids, 1, 5 * 1000, &wait_results);
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
                            bool is_direct_call, uint64_t max_reconstructions) {
    std::unique_ptr<ActorHandle> actor_handle;

    // Test creating actor.
    uint8_t array[] = {1, 2, 3};
    auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));

    RayFunction func{ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "", "", "actor creation task", "")};
    std::vector<TaskArg> args;
    args.emplace_back(TaskArg::PassByValue(
        std::make_shared<RayObject>(buffer, nullptr, std::vector<ObjectID>())));

    ActorCreationOptions actor_options{
        max_reconstructions,
        /*max_concurrency*/ 1, resources,           resources, {},
        /*is_detached*/ false, /*is_asyncio*/ false};

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

    CoreWorkerOptions options = {
        WorkerType::DRIVER,             // worker_type
        Language::PYTHON,               // langauge
        raylet_store_socket_names_[0],  // store_socket
        raylet_socket_names_[0],        // raylet_socket
        NextJobId(),                    // job_id
        gcs_options_,                   // gcs_options
        "",                             // log_dir
        true,                           // install_failure_signal_handler
        "127.0.0.1",                    // node_ip_address
        node_manager_port_,             // node_manager_port
        "127.0.0.1",                    // raylet_ip_address
        "queue_tests",                  // driver_name
        "",                             // stdout_file
        "",                             // stderr_file
        nullptr,                        // task_execution_callback
        nullptr,                        // check_signals
        nullptr,                        // gc_collect
        nullptr,                        // get_lang_stack
        nullptr,                        // kill_main
        true,                           // ref_counting_enabled
        false,                          // is_local_mode
        1,                              // num_workers
    };
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
  std::string raylet_executable_;
  std::string gcs_server_executable_;
  std::string store_executable_;
  std::string actor_executable_;
  int node_manager_port_;
  std::string gcs_server_pid_;
};

}  // namespace streaming
}  // namespace ray

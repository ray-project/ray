

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
                         std::string actor_exe)
      : gcs_options_("127.0.0.1", 6379, ""),
        raylet_executable_(raylet_exe),
        store_executable_(store_exe),
        actor_executable_(actor_exe) {
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

    // start raylet on each node. Assign each node with different resources so that
    // a task can be scheduled to the desired node.
    for (int i = 0; i < num_nodes; i++) {
      raylet_socket_names_[i] =
          StartRaylet(raylet_store_socket_names_[i], "127.0.0.1", "127.0.0.1",
                      "\"CPU,4.0,resource" + std::to_string(i) + ",10\"");
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
  }

  JobID NextJobId() const {
    static uint32_t job_counter = 1;
    return JobID::FromInt(job_counter++);
  }

  std::string StartStore() {
    std::string store_socket_name = "/tmp/store" + RandomObjectID().Hex();
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

  std::string StartRaylet(std::string store_socket_name, std::string node_ip_address,
                          std::string redis_address, std::string resource) {
    std::string raylet_socket_name = "/tmp/raylet" + RandomObjectID().Hex();
    std::string ray_start_cmd = raylet_executable_;
    ray_start_cmd.append(" --raylet_socket_name=" + raylet_socket_name)
        .append(" --store_socket_name=" + store_socket_name)
        .append(" --object_manager_port=0 --node_manager_port=0")
        .append(" --node_ip_address=" + node_ip_address)
        .append(" --redis_address=" + redis_address)
        .append(" --redis_port=6379")
        .append(" --num_initial_workers=1")
        .append(" --maximum_startup_concurrency=10")
        .append(" --static_resource_list=" + resource)
        .append(" --python_worker_command=\"" + actor_executable_ + " " +
                store_socket_name + " " + raylet_socket_name + "\"")
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

  void InitWorker(CoreWorker &driver, ActorHandle &self_actor_handle,
                  ActorHandle &peer_actor_handle,
                  const queue::flatbuf::StreamingQueueTestRole role,
                  const std::vector<ObjectID> &queue_ids,
                  const std::vector<ObjectID> &rescale_queue_ids, std::string suite_name,
                  std::string test_name, uint64_t param) {
    std::string forked_serialized_str;
    peer_actor_handle.Fork().Serialize(&forked_serialized_str);
    STREAMING_LOG(INFO) << "forked_serialized_str: " << forked_serialized_str;
    TestInitMsg msg(role, self_actor_handle.ActorID(), peer_actor_handle.ActorID(),
                    forked_serialized_str, queue_ids, rescale_queue_ids, suite_name,
                    test_name, param);

    std::vector<TaskArg> args;
    args.emplace_back(
        TaskArg::PassByValue(std::make_shared<RayObject>(msg.ToBytes(), nullptr, true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options{0, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func{ray::Language::PYTHON, {"init"}};

    RAY_CHECK_OK(driver.Tasks().SubmitActorTask(self_actor_handle, func, args, options,
                                                &return_ids));
  }

  void SubmitTestToActor(CoreWorker &driver, ActorHandle &actor_handle,
                         const std::string test) {
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);
    std::vector<TaskArg> args;
    args.emplace_back(
        TaskArg::PassByValue(std::make_shared<RayObject>(buffer, nullptr, true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options{0, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func{ray::Language::PYTHON, {"execute_test", test}};

    RAY_CHECK_OK(
        driver.Tasks().SubmitActorTask(actor_handle, func, args, options, &return_ids));
  }

  bool CheckCurTest(CoreWorker &driver, ActorHandle &actor_handle,
                    const std::string test_name) {
    uint8_t data[8];
    auto buffer = std::make_shared<LocalMemoryBuffer>(data, 8, true);
    std::vector<TaskArg> args;
    args.emplace_back(
        TaskArg::PassByValue(std::make_shared<RayObject>(buffer, nullptr, true)));
    std::unordered_map<std::string, double> resources;
    TaskOptions options{1, resources};
    std::vector<ObjectID> return_ids;
    RayFunction func{ray::Language::PYTHON, {"check_current_test_status"}};

    RAY_CHECK_OK(
        driver.Tasks().SubmitActorTask(actor_handle, func, args, options, &return_ids));

    std::vector<bool> wait_results;
    std::vector<std::shared_ptr<RayObject>> results;
    Status wait_st = driver.Objects().Wait(return_ids, 1, 5 * 1000, &wait_results);
    if (!wait_st.ok()) {
      STREAMING_LOG(ERROR) << "Wait fail.";
      return false;
    }
    STREAMING_CHECK(wait_results.size() >= 1);
    if (!wait_results[0]) {
      STREAMING_LOG(WARNING) << "Wait direct call fail.";
      return false;
    }

    Status get_st = driver.Objects().Get(return_ids, -1, &results);
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
    queue::flatbuf::MessageType *type = (queue::flatbuf::MessageType *)p_cur;
    STREAMING_CHECK(*type ==
                    queue::flatbuf::MessageType::StreamingQueueTestCheckStatusRspMsg);
    std::shared_ptr<TestCheckStatusRspMsg> message =
        TestCheckStatusRspMsg::FromBytes(bytes);
    STREAMING_CHECK(message->TestName() == test_name);
    return message->Status();
  }

  std::unique_ptr<ActorHandle> CreateActorHelper(
      CoreWorker &worker, const std::unordered_map<std::string, double> &resources,
      bool is_direct_call, uint64_t max_reconstructions) {
    std::unique_ptr<ActorHandle> actor_handle;

    // Test creating actor.
    uint8_t array[] = {1, 2, 3};
    auto buffer = std::make_shared<LocalMemoryBuffer>(array, sizeof(array));

    RayFunction func{ray::Language::PYTHON, {"actor creation task"}};
    std::vector<TaskArg> args;
    args.emplace_back(TaskArg::PassByValue(std::make_shared<RayObject>(buffer, nullptr)));

    ActorCreationOptions actor_options{
        max_reconstructions, is_direct_call, resources, {}};

    // Create an actor.
    RAY_CHECK_OK(worker.Tasks().CreateActor(func, args, actor_options, &actor_handle));
    return actor_handle;
  }

  void SubmitTest(uint32_t queue_num, std::string suite_name, std::string test_name,
                  uint64_t timeout_ms) {
    std::vector<ray::ObjectID> queue_id_vec;
    std::vector<ray::ObjectID> rescale_queue_id_vec;
    for (uint32_t i = 0; i < queue_num; ++i) {
      ObjectID queue_id = ray::ObjectID::FromRandom();
      ConvertToValidQueueId(queue_id);
      queue_id_vec.emplace_back(queue_id);
    }

    // One scale id
    ObjectID rescale_queue_id = ray::ObjectID::FromRandom();
    ConvertToValidQueueId(rescale_queue_id);
    rescale_queue_id_vec.emplace_back(rescale_queue_id);

    std::vector<uint64_t> channel_seq_id_vec(queue_num, 0);

    for (size_t i = 0; i < queue_id_vec.size(); ++i) {
      STREAMING_LOG(INFO) << " qid hex => " << queue_id_vec[i].Hex();
    }
    for (auto &qid : rescale_queue_id_vec) {
      STREAMING_LOG(INFO) << " rescale qid hex => " << qid.Hex();
    }
    STREAMING_LOG(INFO) << "Sub process: writer.";

    CoreWorker driver(WorkerType::DRIVER, Language::PYTHON, raylet_store_socket_names_[0],
                      raylet_socket_names_[0], NextJobId(), gcs_options_, "", nullptr);

    // Create writer and reader actors
    std::unordered_map<std::string, double> resources;
    auto actor_handle_writer = CreateActorHelper(driver, resources, true, 0);
    auto actor_handle_reader = CreateActorHelper(driver, resources, true, 0);

    InitWorker(driver, *actor_handle_writer, *actor_handle_reader,
               queue::flatbuf::StreamingQueueTestRole::WRITER, queue_id_vec,
               rescale_queue_id_vec, suite_name, test_name, GetParam());
    InitWorker(driver, *actor_handle_reader, *actor_handle_writer,
               queue::flatbuf::StreamingQueueTestRole::READER, queue_id_vec,
               rescale_queue_id_vec, suite_name, test_name, GetParam());

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    SubmitTestToActor(driver, *actor_handle_writer, test_name);
    SubmitTestToActor(driver, *actor_handle_reader, test_name);

    uint64_t slept_time_ms = 0;
    while (slept_time_ms < timeout_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5*1000));
      STREAMING_LOG(INFO) << "Check test status.";
      if (CheckCurTest(driver, *actor_handle_writer, test_name) &&
          CheckCurTest(driver, *actor_handle_reader, test_name)) {
        STREAMING_LOG(INFO) << "Test Success, Exit.";
        return;
      }
      slept_time_ms += 5*1000;
    }

    EXPECT_TRUE(false);
    STREAMING_LOG(INFO) << "Test Timeout, Exit.";
  }

  void SetUp() {}

  void TearDown() {}

  // Test tore provider.
  void TestStoreProvider(StoreProviderType type);

  // Test normal tasks.
  void TestNormalTask(const std::unordered_map<std::string, double> &resources);

  // Test actor tasks.
  void TestActorTask(const std::unordered_map<std::string, double> &resources,
                     bool is_direct_call);

  // Test actor failure case, verify that the tasks would either succeed or
  // fail with exceptions, in that case the return objects fetched from `Get`
  // contain errors.
  void TestActorFailure(const std::unordered_map<std::string, double> &resources,
                        bool is_direct_call);

  // Test actor failover case. Verify that actor can be reconstructed successfully,
  // and as long as we wait for actor reconstruction before submitting new tasks,
  // it is guaranteed that all tasks are successfully completed.
  void TestActorReconstruction(const std::unordered_map<std::string, double> &resources,
                               bool is_direct_call);

  // Test actor performance.
  void TestActorPerformance(const std::unordered_map<std::string, double> &resources,
                            bool is_direct_call, bool use_no_returns);

  void TestWaitMultipleActorCreations(
      const std::unordered_map<std::string, double> &resources, bool is_direct_call);

 protected:
  bool WaitForDirectCallActorState(CoreWorker &worker, const ActorID &actor_id,
                                   bool wait_alive, int timeout_ms);

  std::vector<std::string> raylet_socket_names_;
  std::vector<std::string> raylet_store_socket_names_;
  gcs::GcsClientOptions gcs_options_;
  std::string raylet_executable_;
  std::string store_executable_;
  std::string actor_executable_;
};

}  // namespace streaming
}  // namespace ray

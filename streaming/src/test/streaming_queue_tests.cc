#define BOOST_BIND_NO_PLACEHOLDERS
#include "data_reader.h"
#include "data_writer.h"
#include "gtest/gtest.h"
#include "message/message.h"
#include "message/message_bundle.h"
#include "queue/queue_client.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/core_worker.h"
#include "ring_buffer/ring_buffer.h"
#include "test/queue_tests_base.h"

using namespace std::placeholders;
namespace ray {
namespace streaming {

static int node_manager_port;

class StreamingQueueTest : public StreamingQueueTestBase {
 public:
  StreamingQueueTest() : StreamingQueueTestBase(1, node_manager_port) {}
};

class StreamingWriterTest : public StreamingQueueTestBase {
 public:
  StreamingWriterTest() : StreamingQueueTestBase(1, node_manager_port) {}
};

class StreamingExactlySameTest : public StreamingQueueTestBase {
 public:
  StreamingExactlySameTest() : StreamingQueueTestBase(1, node_manager_port) {}
};

TEST_P(StreamingQueueTest, PullPeerAsyncTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.pull_peer_async_test";

  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "pull_peer_async_test", 60 * 1000);
}

TEST_P(StreamingQueueTest, GetQueueTest) {
  STREAMING_LOG(INFO) << "StreamingQueueTest.get_queue_test";

  uint32_t queue_num = 1;
  SubmitTest(queue_num, "StreamingQueueTest", "get_queue_test", 60 * 1000);
}

TEST_P(StreamingWriterTest, streaming_writer_exactly_once_test) {
  STREAMING_LOG(INFO) << "StreamingWriterTest.streaming_writer_exactly_once_test";

  uint32_t queue_num = 1;

  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY ONCE";
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_writer_exactly_once_test",
             60 * 1000);
}

INSTANTIATE_TEST_CASE_P(StreamingTest, StreamingQueueTest, testing::Values(0));

INSTANTIATE_TEST_CASE_P(StreamingTest, StreamingWriterTest, testing::Values(0));

INSTANTIATE_TEST_CASE_P(StreamingTest, StreamingExactlySameTest,
                        testing::Values(0, 1, 5, 9));

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 8);
  ray::TEST_RAYLET_EXEC_PATH = std::string(argv[1]);
  ray::streaming::node_manager_port = std::stoi(std::string(argv[2]));
  ray::TEST_MOCK_WORKER_EXEC_PATH = std::string(argv[3]);
  ray::TEST_GCS_SERVER_EXEC_PATH = std::string(argv[4]);
  ray::TEST_REDIS_SERVER_EXEC_PATH = std::string(argv[5]);
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = std::string(argv[6]);
  ray::TEST_REDIS_CLIENT_EXEC_PATH = std::string(argv[7]);
  return RUN_ALL_TESTS();
}

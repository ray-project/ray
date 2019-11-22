#define BOOST_BIND_NO_PLACEHOLDERS
#include <unistd.h>
#include "gtest/gtest.h"
#include "queue/streaming_queue_client.h"
#include "ray/core_worker/core_worker.h"

#include "streaming.h"
#include "message/streaming_message.h"
#include "message/streaming_message_bundle.h"
#include "streaming_reader.h"
#include "streaming_ring_buffer.h"
#include "streaming_writer.h"

#include "streaming_queue_tests_base.h"

using namespace std::placeholders;
namespace ray {
namespace streaming {

static std::string store_executable;
static std::string raylet_executable;
static std::string actor_executable;

class StreamingWriterTest : public StreamingQueueTestBase {
 public:
  StreamingWriterTest()
      : StreamingQueueTestBase(1, raylet_executable, store_executable, actor_executable) {
  }
};

class StreamingExactlySameTest : public StreamingQueueTestBase {
 public:
  StreamingExactlySameTest()
      : StreamingQueueTestBase(1, raylet_executable, store_executable, actor_executable) {
  }
};

TEST_P(StreamingWriterTest, streaming_writer_exactly_once_test) {
  STREAMING_LOG(INFO) << "StreamingWriterTest.streaming_writer_exactly_once_test";

  uint32_t queue_num = 1;

  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY ONCE";
  SubmitTest(queue_num, "StreamingWriterTest", "streaming_writer_exactly_once_test",
             60 * 1000);
}

INSTANTIATE_TEST_CASE_P(StreamingTest, StreamingWriterTest, testing::Values(0));

INSTANTIATE_TEST_CASE_P(StreamingTest, StreamingExactlySameTest,
                        testing::Values(0, 1, 5, 9));

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  // set_streaming_log_config("streaming_writer_test", StreamingLogLevel::INFO, 0);
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::streaming::store_executable = std::string(argv[1]);
  ray::streaming::raylet_executable = std::string(argv[2]);
  ray::streaming::actor_executable = std::string(argv[3]);
  return RUN_ALL_TESTS();
}

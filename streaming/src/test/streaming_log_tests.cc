#include "gtest/gtest.h"

#include "streaming_logging.h"

using namespace ray;
using namespace ray::streaming;

TEST(StreamingLogTest, log_test) {
  StreamingLog::StartStreamingLog("streaming-test", StreamingLogLevel::DEBUG, 0,
                                  "/tmp/streaminglogs/");
  STREAMING_LOG(DEBUG) << "DEBUG";
  STREAMING_LOG(INFO) << "INFO";
  STREAMING_LOG(WARNING) << "WARNING";
  STREAMING_LOG(ERROR) << "ERROR";
  StreamingLog::ShutDownStreamingLog();

  StreamingLog::StartStreamingLog("streaming-test", StreamingLogLevel::DEBUG, 0,
                                  "/tmp/streaminglogs/");
  STREAMING_LOG(DEBUG) << "DEBUG2";
  STREAMING_LOG(DEBUG) << "DEBUG3";
  STREAMING_LOG(INFO) << "INFO2";
  STREAMING_LOG(INFO) << "INFO3";
  STREAMING_LOG(WARNING) << "WARNING2";
  STREAMING_LOG(WARNING) << "WARNING3";
  STREAMING_LOG(ERROR) << "ERROR2";

  STREAMING_LOG(DEBUG) << "DEBUG2";
  STREAMING_LOG(DEBUG) << "DEBUG3";
  STREAMING_LOG(INFO) << "INFO2";
  STREAMING_LOG(INFO) << "INFO3";
  STREAMING_LOG(WARNING) << "WARNING2";
  STREAMING_LOG(WARNING) << "WARNING3";
  STREAMING_LOG(ERROR) << "ERROR2";
  StreamingLog::ShutDownStreamingLog();

  StreamingLog::StartStreamingLog("streaming-test", StreamingLogLevel::INFO, 0,
                                  "/tmp/streaminglogs/");
  STREAMING_LOG(DEBUG) << "DEBUG4";
  STREAMING_LOG(DEBUG) << "DEBUG5";
  STREAMING_LOG(INFO) << "INFO4";
  STREAMING_LOG(INFO) << "INFO5";
  STREAMING_LOG(WARNING) << "WARNING4";
  STREAMING_LOG(WARNING) << "WARNING5";
  STREAMING_LOG(ERROR) << "ERROR4";
  StreamingLog::ShutDownStreamingLog();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#include "gtest/gtest.h"

#include "format/streaming_generated.h"
#include "streaming.h"
#include "streaming_writer.h"

using namespace ray;
using namespace ray::streaming;

class StreamingFbsTest : public ::testing::Test {
  virtual void SetUp() {
    set_streaming_log_config("streaming_fbs_tests", StreamingLogLevel::WARNING);
    base = new StreamingWriter();
  }

  virtual void TearDown() { delete base; }

 protected:
  StreamingCommon *base;
};

TEST_F(StreamingFbsTest, streaming_fbs_test) {
  flatbuffers::FlatBufferBuilder builder(1024);

  auto uint_pair = streaming::fbs::CreateStreamingUintPair(
      builder, streaming::fbs::StreamingConfigKey::StreamingStrategy,
      static_cast<uint32_t>(StreamingStrategy::EXACTLY_SAME));

  std::vector<flatbuffers::Offset<streaming::fbs::StreamingUintPair>> pair_vec;
  pair_vec.push_back(uint_pair);
  std::string persistence_path = "/tmp/test/job";
  auto str_path = builder.CreateString(persistence_path);

  auto str_pair = streaming::fbs::CreateStreamingStringPair(
      builder, streaming::fbs::StreamingConfigKey::StreamingPersistencePath, str_path);

  std::vector<flatbuffers::Offset<streaming::fbs::StreamingStringPair>> str_pair_vec;
  str_pair_vec.push_back(str_pair);

  auto task_driver_id = builder.CreateString("3030303030303030adebc6fe3030303030313032");
  auto config = streaming::fbs::CreateStreamingConfig(
      builder, builder.CreateVector(str_pair_vec), builder.CreateVector(pair_vec),
      streaming::fbs::StreamingRole::Sink, 0, task_driver_id);
  builder.Finish(config);

  base->SetConfig(builder.GetBufferPointer(), builder.GetSize());

  EXPECT_TRUE(base->GetConfig().GetStreaming_strategy_() ==
              StreamingStrategy::EXACTLY_SAME);
  EXPECT_TRUE(base->GetConfig().GetStreaming_persistence_path() == persistence_path);
  EXPECT_TRUE(base->GetConfig().GetStreaming_role() ==
              streaming::fbs::StreamingRole::Sink);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

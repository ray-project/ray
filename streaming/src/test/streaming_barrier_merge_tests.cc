#include <unistd.h>

#include "gtest/gtest.h"
#include "streaming.h"
#include "streaming_message_merger.h"
#include "streaming_reader.h"

using namespace ray::streaming;

TEST(StreamingBarrierMergerTest, streaming_barrier_merge_test) {
  StreamingReaderMsgPtrComparator comparator;
  StreamingMessageMerger<std::shared_ptr<StreamingReaderBundle>,
                         StreamingReaderMsgPtrComparator>
      merger(comparator);
  std::shared_ptr<StreamingReaderBundle> item = std::make_shared<StreamingReaderBundle>();
  merger.push(item);
  auto result = merger.top();
  merger.pop();
  EXPECT_TRUE(result->from.Hex() == item->from.Hex());
}

TEST(StreamingBarrierMergerTest, streaming_barrier_exactly_once_merge_test) {
  StreamingReaderMsgPtrComparator comparator(StreamingStrategy::EXACTLY_ONCE);
  StreamingMessageMerger<std::shared_ptr<StreamingReaderBundle>,
                         StreamingReaderMsgPtrComparator>
      merger(comparator);

  std::vector<ray::ObjectID> id_list{ray::ObjectID::FromRandom(),
                                     ray::ObjectID::FromRandom()};

  std::vector<uint32_t> last_checkpoint_ids1{0, 1, 1, 1};

  std::vector<uint32_t> last_checkpoint_ids2{0, 0, 0, 1};

  for (uint32_t i = 0; i < 4; ++i) {
    for (uint32_t j = 0; j < 2; ++j) {
      std::shared_ptr<StreamingReaderBundle> item =
          std::make_shared<StreamingReaderBundle>();
      item->from = id_list[j];

      if (j == 0) {
        item->last_barrier_id = last_checkpoint_ids1[i];
      } else {
        item->last_barrier_id = last_checkpoint_ids2[i];
      }
      StreamingMessageBundle bundle(i, i);
      item->meta = std::make_shared<StreamingMessageBundle>(bundle);
      merger.push(item);
    }
  }
  uint32_t vec_size = merger.size();

  uint32_t id2_continueous_num = 0;
  uint32_t max_continueous_num = 0;

  for (uint32_t i = 0; i < vec_size; ++i) {
    auto result = merger.top();
    if (result->from == id_list[1]) {
      id2_continueous_num++;
    } else {
      id2_continueous_num = 0;
    }
    max_continueous_num = std::max(id2_continueous_num, max_continueous_num);
    STREAMING_LOG(DEBUG) << result->from.Hex() << ","
                         << result->meta->GetMessageBundleTs() << ","
                         << result->meta->GetLastMessageId() << ","
                         << result->last_barrier_id;
    merger.pop();
  }
  EXPECT_TRUE(max_continueous_num >= 2) << max_continueous_num;
}
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

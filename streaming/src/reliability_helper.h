#pragma once
#include "channel/channel.h"
#include "data_reader.h"
#include "data_writer.h"
#include "reliability/barrier_helper.h"
#include "util/config.h"

namespace ray {
namespace streaming {

class ReliabilityHelper;
class DataWriter;
class DataReader;

class ReliabilityHelperFactory {
 public:
  static std::shared_ptr<ReliabilityHelper> CreateReliabilityHelper(
      const StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
      DataWriter *writer, DataReader *reader);
};

class ReliabilityHelper {
 public:
  ReliabilityHelper(const StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
                    DataWriter *writer, DataReader *reader);
  virtual ~ReliabilityHelper() = default;
  // Only exactly same need override this function.
  virtual void Reload();
  // Store bundle meta or skip in replay mode.
  virtual bool StoreBundleMeta(ProducerChannelInfo &channel_info,
                               StreamingMessageBundlePtr &bundle_ptr,
                               bool is_replay = false);
  virtual void CleanupCheckpoint(ProducerChannelInfo &channel_info, uint64_t barrier_id);
  // Filter message by different failover strategies.
  virtual bool FilterMessage(ProducerChannelInfo &channel_info, const uint8_t *data,
                             StreamingMessageType message_type,
                             uint64_t *write_message_id);
  virtual StreamingStatus InitChannelMerger(uint32_t timeout);
  virtual StreamingStatus HandleNoValidItem(ConsumerChannelInfo &channel_info);

 protected:
  const StreamingConfig &config_;
  StreamingBarrierHelper &barrier_helper_;
  DataWriter *writer_;
  DataReader *reader_;
};

class AtLeastOnceHelper : public ReliabilityHelper {
 public:
  AtLeastOnceHelper(const StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
                    DataWriter *writer, DataReader *reader);
  StreamingStatus InitChannelMerger(uint32_t timeout) override;
  StreamingStatus HandleNoValidItem(ConsumerChannelInfo &channel_info) override;
};

class ExactlyOnceHelper : public ReliabilityHelper {
 public:
  ExactlyOnceHelper(const StreamingConfig &config, StreamingBarrierHelper &barrier_helper,
                    DataWriter *writer, DataReader *reader);
  bool FilterMessage(ProducerChannelInfo &channel_info, const uint8_t *data,
                     StreamingMessageType message_type,
                     uint64_t *write_message_id) override;
  virtual ~ExactlyOnceHelper() = default;
};
}  // namespace streaming
}  // namespace ray

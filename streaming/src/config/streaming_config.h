#ifndef RAY_STREAMING_CONFIG_H
#define RAY_STREAMING_CONFIG_H

#include <cstdint>
#include <string>

#include "protobuf/streaming.pb.h"
#include "ray/common/id.h"

namespace ray {
namespace streaming {

class StreamingConfig {
 public:
  static uint64_t TIME_WAIT_UINT;
  static uint32_t DEFAULT_RING_BUFFER_CAPACITY;
  static uint32_t DEFAULT_EMPTY_MESSAGE_TIME_INTERVAL;
  static const uint32_t MESSAGE_BUNDLE_MAX_SIZE;

 private:
  uint32_t ring_buffer_capacity_ = DEFAULT_RING_BUFFER_CAPACITY;

  uint32_t empty_message_time_interval_ = DEFAULT_EMPTY_MESSAGE_TIME_INTERVAL;

  streaming::proto::OperatorType operator_type_ =
      streaming::proto::OperatorType::TRANSFORM;

  std::string job_name_ = "DEFAULT_JOB_NAME";

  std::string op_name_ = "DEFAULT_OP_NAME";

  std::string worker_name_ = "DEFAULT_WORKER_NAME";

  std::string task_job_id_ = JobID::Nil().Hex();

 public:
  void FromProto(const uint8_t *, uint32_t size);

  const std::string &GetTaskJobId() const;

  void SetTaskJobId(const std::string &task_job_id);

  const std::string &GetWorkerName() const;

  void SetWorkerName(const std::string &worker_name);

  const std::string &GetOpName() const;

  void SetOpName(const std::string &op_name);

  uint32_t GetEmptyMessageTimeInterval() const;

  void SetEmptyMessageTimeInterval(uint32_t empty_message_time_interval);

  uint32_t GetRingBufferCapacity() const;

  void SetRingBufferCapacity(uint32_t ring_buffer_capacity);

  streaming::proto::OperatorType GetOperatorType() const;

  void SetOperatorType(streaming::proto::OperatorType type);

  const std::string &GetJobName() const;

  void SetJobName(const std::string &job_name);
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_CONFIG_H

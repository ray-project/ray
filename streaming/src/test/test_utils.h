
#ifndef RAY_TEST_UTILS_H
#define RAY_TEST_UTILS_H

#include <unistd.h>
#include "gtest/gtest.h"

#include "streaming.h"
#include "streaming_message.h"
#include "streaming_writer.h"

namespace ray {
namespace streaming {

namespace util {

ray::streaming::Buffer ToMessageBuffer(StreamingWriter *writer, const ray::ObjectID &qid,
                                       uint8_t *data, uint32_t size);

StreamingMessagePtr MakeMessagePtr(const uint8_t *, uint32_t, uint64_t,
                                   StreamingMessageType);

}  // namespace util
}  // namespace streaming
}  // namespace ray

#endif  // RAY_TEST_UTILS_H

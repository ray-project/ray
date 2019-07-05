// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_queue_runner_proto_IMPL_H_
#define tensorflow_core_protobuf_queue_runner_proto_IMPL_H_

#include "tensorflow/core/lib/core/error_codes.pb.h"
#include "tensorflow/core/lib/core/error_codes.pb_text-impl.h"
#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"
#include "tensorflow/core/protobuf/queue_runner.pb.h"
#include "tensorflow/core/protobuf/queue_runner.pb_text.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::QueueRunnerDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::QueueRunnerDef* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_queue_runner_proto_IMPL_H_

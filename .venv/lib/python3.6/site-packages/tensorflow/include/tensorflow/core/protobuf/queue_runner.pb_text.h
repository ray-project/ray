// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_queue_runner_proto_H_
#define tensorflow_core_protobuf_queue_runner_proto_H_

#include "tensorflow/core/protobuf/queue_runner.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.QueueRunnerDef
string ProtoDebugString(
    const ::tensorflow::QueueRunnerDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::QueueRunnerDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::QueueRunnerDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_queue_runner_proto_H_

// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_iterator_proto_H_
#define tensorflow_core_framework_iterator_proto_H_

#include "tensorflow/core/framework/iterator.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.IteratorStateMetadata
string ProtoDebugString(
    const ::tensorflow::IteratorStateMetadata& msg);
string ProtoShortDebugString(
    const ::tensorflow::IteratorStateMetadata& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::IteratorStateMetadata* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_iterator_proto_H_

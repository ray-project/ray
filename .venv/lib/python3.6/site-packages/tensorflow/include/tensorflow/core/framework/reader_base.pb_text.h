// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_reader_base_proto_H_
#define tensorflow_core_framework_reader_base_proto_H_

#include "tensorflow/core/framework/reader_base.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.ReaderBaseState
string ProtoDebugString(
    const ::tensorflow::ReaderBaseState& msg);
string ProtoShortDebugString(
    const ::tensorflow::ReaderBaseState& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ReaderBaseState* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_reader_base_proto_H_

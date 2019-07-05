// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_example_example_proto_H_
#define tensorflow_core_example_example_proto_H_

#include "tensorflow/core/example/example.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.Example
string ProtoDebugString(
    const ::tensorflow::Example& msg);
string ProtoShortDebugString(
    const ::tensorflow::Example& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::Example* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.SequenceExample
string ProtoDebugString(
    const ::tensorflow::SequenceExample& msg);
string ProtoShortDebugString(
    const ::tensorflow::SequenceExample& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SequenceExample* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_example_example_proto_H_

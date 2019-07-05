// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_tensor_description_proto_H_
#define tensorflow_core_framework_tensor_description_proto_H_

#include "tensorflow/core/framework/tensor_description.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.TensorDescription
string ProtoDebugString(
    const ::tensorflow::TensorDescription& msg);
string ProtoShortDebugString(
    const ::tensorflow::TensorDescription& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::TensorDescription* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_tensor_description_proto_H_

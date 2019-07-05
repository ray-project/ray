// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_tensor_slice_proto_H_
#define tensorflow_core_framework_tensor_slice_proto_H_

#include "tensorflow/core/framework/tensor_slice.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.TensorSliceProto.Extent
string ProtoDebugString(
    const ::tensorflow::TensorSliceProto_Extent& msg);
string ProtoShortDebugString(
    const ::tensorflow::TensorSliceProto_Extent& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::TensorSliceProto_Extent* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.TensorSliceProto
string ProtoDebugString(
    const ::tensorflow::TensorSliceProto& msg);
string ProtoShortDebugString(
    const ::tensorflow::TensorSliceProto& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::TensorSliceProto* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_tensor_slice_proto_H_

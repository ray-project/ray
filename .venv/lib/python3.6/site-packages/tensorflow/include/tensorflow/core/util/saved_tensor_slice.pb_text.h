// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_util_saved_tensor_slice_proto_H_
#define tensorflow_core_util_saved_tensor_slice_proto_H_

#include "tensorflow/core/util/saved_tensor_slice.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.SavedSliceMeta
string ProtoDebugString(
    const ::tensorflow::SavedSliceMeta& msg);
string ProtoShortDebugString(
    const ::tensorflow::SavedSliceMeta& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SavedSliceMeta* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.SavedTensorSliceMeta
string ProtoDebugString(
    const ::tensorflow::SavedTensorSliceMeta& msg);
string ProtoShortDebugString(
    const ::tensorflow::SavedTensorSliceMeta& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SavedTensorSliceMeta* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.SavedSlice
string ProtoDebugString(
    const ::tensorflow::SavedSlice& msg);
string ProtoShortDebugString(
    const ::tensorflow::SavedSlice& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SavedSlice* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.SavedTensorSlices
string ProtoDebugString(
    const ::tensorflow::SavedTensorSlices& msg);
string ProtoShortDebugString(
    const ::tensorflow::SavedTensorSlices& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SavedTensorSlices* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_util_saved_tensor_slice_proto_H_

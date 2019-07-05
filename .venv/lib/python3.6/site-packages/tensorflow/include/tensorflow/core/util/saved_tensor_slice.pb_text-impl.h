// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_util_saved_tensor_slice_proto_IMPL_H_
#define tensorflow_core_util_saved_tensor_slice_proto_IMPL_H_

#include "tensorflow/core/framework/resource_handle.pb.h"
#include "tensorflow/core/framework/resource_handle.pb_text-impl.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/tensor.pb_text-impl.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_shape.pb_text-impl.h"
#include "tensorflow/core/framework/tensor_slice.pb.h"
#include "tensorflow/core/framework/tensor_slice.pb_text-impl.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/types.pb_text-impl.h"
#include "tensorflow/core/framework/versions.pb.h"
#include "tensorflow/core/framework/versions.pb_text-impl.h"
#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"
#include "tensorflow/core/util/saved_tensor_slice.pb.h"
#include "tensorflow/core/util/saved_tensor_slice.pb_text.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SavedSliceMeta& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SavedSliceMeta* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SavedTensorSliceMeta& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SavedTensorSliceMeta* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SavedSlice& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SavedSlice* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SavedTensorSlices& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SavedTensorSlices* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_util_saved_tensor_slice_proto_IMPL_H_

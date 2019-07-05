// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_tensor_bundle_proto_IMPL_H_
#define tensorflow_core_protobuf_tensor_bundle_proto_IMPL_H_

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
#include "tensorflow/core/protobuf/tensor_bundle.pb.h"
#include "tensorflow/core/protobuf/tensor_bundle.pb_text.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::BundleHeaderProto& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::BundleHeaderProto* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::BundleEntryProto& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::BundleEntryProto* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_tensor_bundle_proto_IMPL_H_

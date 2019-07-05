// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_tensor_bundle_proto_H_
#define tensorflow_core_protobuf_tensor_bundle_proto_H_

#include "tensorflow/core/protobuf/tensor_bundle.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Enum text output for tensorflow.BundleHeaderProto.Endianness
const char* EnumName_BundleHeaderProto_Endianness(
    ::tensorflow::BundleHeaderProto_Endianness value);

// Message-text conversion for tensorflow.BundleHeaderProto
string ProtoDebugString(
    const ::tensorflow::BundleHeaderProto& msg);
string ProtoShortDebugString(
    const ::tensorflow::BundleHeaderProto& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::BundleHeaderProto* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.BundleEntryProto
string ProtoDebugString(
    const ::tensorflow::BundleEntryProto& msg);
string ProtoShortDebugString(
    const ::tensorflow::BundleEntryProto& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::BundleEntryProto* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_tensor_bundle_proto_H_

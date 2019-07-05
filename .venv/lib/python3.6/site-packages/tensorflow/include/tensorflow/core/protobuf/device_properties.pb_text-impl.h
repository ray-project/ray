// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_device_properties_proto_IMPL_H_
#define tensorflow_core_protobuf_device_properties_proto_IMPL_H_

#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"
#include "tensorflow/core/protobuf/device_properties.pb.h"
#include "tensorflow/core/protobuf/device_properties.pb_text.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::DeviceProperties& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::DeviceProperties* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::NamedDevice& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::NamedDevice* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_device_properties_proto_IMPL_H_

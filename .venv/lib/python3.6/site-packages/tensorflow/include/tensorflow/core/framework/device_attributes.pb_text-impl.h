// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_device_attributes_proto_IMPL_H_
#define tensorflow_core_framework_device_attributes_proto_IMPL_H_

#include "tensorflow/core/framework/device_attributes.pb.h"
#include "tensorflow/core/framework/device_attributes.pb_text.h"
#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::InterconnectLink& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::InterconnectLink* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::LocalLinks& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::LocalLinks* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::DeviceLocality& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::DeviceLocality* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::DeviceAttributes& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::DeviceAttributes* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_framework_device_attributes_proto_IMPL_H_

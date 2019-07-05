// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_debug_proto_IMPL_H_
#define tensorflow_core_protobuf_debug_proto_IMPL_H_

#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"
#include "tensorflow/core/protobuf/debug.pb.h"
#include "tensorflow/core/protobuf/debug.pb_text.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::DebugTensorWatch& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::DebugTensorWatch* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::DebugOptions& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::DebugOptions* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::DebuggedSourceFile& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::DebuggedSourceFile* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::DebuggedSourceFiles& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::DebuggedSourceFiles* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_debug_proto_IMPL_H_

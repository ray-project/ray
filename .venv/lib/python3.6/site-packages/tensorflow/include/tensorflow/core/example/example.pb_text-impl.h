// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_example_example_proto_IMPL_H_
#define tensorflow_core_example_example_proto_IMPL_H_

#include "tensorflow/core/example/example.pb.h"
#include "tensorflow/core/example/example.pb_text.h"
#include "tensorflow/core/example/feature.pb.h"
#include "tensorflow/core/example/feature.pb_text-impl.h"
#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::Example& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::Example* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SequenceExample& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SequenceExample* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_example_example_proto_IMPL_H_

// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_util_memmapped_file_system_proto_IMPL_H_
#define tensorflow_core_util_memmapped_file_system_proto_IMPL_H_

#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"
#include "tensorflow/core/util/memmapped_file_system.pb.h"
#include "tensorflow/core/util/memmapped_file_system.pb_text.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::MemmappedFileSystemDirectoryElement& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::MemmappedFileSystemDirectoryElement* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::MemmappedFileSystemDirectory& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::MemmappedFileSystemDirectory* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_util_memmapped_file_system_proto_IMPL_H_

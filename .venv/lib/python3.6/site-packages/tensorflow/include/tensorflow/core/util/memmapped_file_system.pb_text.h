// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_util_memmapped_file_system_proto_H_
#define tensorflow_core_util_memmapped_file_system_proto_H_

#include "tensorflow/core/util/memmapped_file_system.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.MemmappedFileSystemDirectoryElement
string ProtoDebugString(
    const ::tensorflow::MemmappedFileSystemDirectoryElement& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemmappedFileSystemDirectoryElement& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemmappedFileSystemDirectoryElement* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.MemmappedFileSystemDirectory
string ProtoDebugString(
    const ::tensorflow::MemmappedFileSystemDirectory& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemmappedFileSystemDirectory& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemmappedFileSystemDirectory* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_util_memmapped_file_system_proto_H_

// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_debug_proto_H_
#define tensorflow_core_protobuf_debug_proto_H_

#include "tensorflow/core/protobuf/debug.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.DebugTensorWatch
string ProtoDebugString(
    const ::tensorflow::DebugTensorWatch& msg);
string ProtoShortDebugString(
    const ::tensorflow::DebugTensorWatch& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::DebugTensorWatch* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.DebugOptions
string ProtoDebugString(
    const ::tensorflow::DebugOptions& msg);
string ProtoShortDebugString(
    const ::tensorflow::DebugOptions& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::DebugOptions* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.DebuggedSourceFile
string ProtoDebugString(
    const ::tensorflow::DebuggedSourceFile& msg);
string ProtoShortDebugString(
    const ::tensorflow::DebuggedSourceFile& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::DebuggedSourceFile* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.DebuggedSourceFiles
string ProtoDebugString(
    const ::tensorflow::DebuggedSourceFiles& msg);
string ProtoShortDebugString(
    const ::tensorflow::DebuggedSourceFiles& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::DebuggedSourceFiles* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_debug_proto_H_

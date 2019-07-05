// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_rewriter_config_proto_H_
#define tensorflow_core_protobuf_rewriter_config_proto_H_

#include "tensorflow/core/protobuf/rewriter_config.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.AutoParallelOptions
string ProtoDebugString(
    const ::tensorflow::AutoParallelOptions& msg);
string ProtoShortDebugString(
    const ::tensorflow::AutoParallelOptions& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::AutoParallelOptions* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.ScopedAllocatorOptions
string ProtoDebugString(
    const ::tensorflow::ScopedAllocatorOptions& msg);
string ProtoShortDebugString(
    const ::tensorflow::ScopedAllocatorOptions& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ScopedAllocatorOptions* msg)
        TF_MUST_USE_RESULT;

// Enum text output for tensorflow.RewriterConfig.Toggle
const char* EnumName_RewriterConfig_Toggle(
    ::tensorflow::RewriterConfig_Toggle value);

// Enum text output for tensorflow.RewriterConfig.NumIterationsType
const char* EnumName_RewriterConfig_NumIterationsType(
    ::tensorflow::RewriterConfig_NumIterationsType value);

// Enum text output for tensorflow.RewriterConfig.MemOptType
const char* EnumName_RewriterConfig_MemOptType(
    ::tensorflow::RewriterConfig_MemOptType value);

// Message-text conversion for tensorflow.RewriterConfig.CustomGraphOptimizer
string ProtoDebugString(
    const ::tensorflow::RewriterConfig_CustomGraphOptimizer& msg);
string ProtoShortDebugString(
    const ::tensorflow::RewriterConfig_CustomGraphOptimizer& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::RewriterConfig_CustomGraphOptimizer* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.RewriterConfig
string ProtoDebugString(
    const ::tensorflow::RewriterConfig& msg);
string ProtoShortDebugString(
    const ::tensorflow::RewriterConfig& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::RewriterConfig* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_rewriter_config_proto_H_

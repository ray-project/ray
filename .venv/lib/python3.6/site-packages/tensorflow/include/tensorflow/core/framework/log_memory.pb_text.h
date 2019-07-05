// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_log_memory_proto_H_
#define tensorflow_core_framework_log_memory_proto_H_

#include "tensorflow/core/framework/log_memory.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.MemoryLogStep
string ProtoDebugString(
    const ::tensorflow::MemoryLogStep& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemoryLogStep& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemoryLogStep* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.MemoryLogTensorAllocation
string ProtoDebugString(
    const ::tensorflow::MemoryLogTensorAllocation& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemoryLogTensorAllocation& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemoryLogTensorAllocation* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.MemoryLogTensorDeallocation
string ProtoDebugString(
    const ::tensorflow::MemoryLogTensorDeallocation& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemoryLogTensorDeallocation& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemoryLogTensorDeallocation* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.MemoryLogTensorOutput
string ProtoDebugString(
    const ::tensorflow::MemoryLogTensorOutput& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemoryLogTensorOutput& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemoryLogTensorOutput* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.MemoryLogRawAllocation
string ProtoDebugString(
    const ::tensorflow::MemoryLogRawAllocation& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemoryLogRawAllocation& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemoryLogRawAllocation* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.MemoryLogRawDeallocation
string ProtoDebugString(
    const ::tensorflow::MemoryLogRawDeallocation& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemoryLogRawDeallocation& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemoryLogRawDeallocation* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_log_memory_proto_H_

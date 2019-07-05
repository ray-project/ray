// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_log_memory_proto_IMPL_H_
#define tensorflow_core_framework_log_memory_proto_IMPL_H_

#include "tensorflow/core/framework/allocation_description.pb.h"
#include "tensorflow/core/framework/allocation_description.pb_text-impl.h"
#include "tensorflow/core/framework/log_memory.pb.h"
#include "tensorflow/core/framework/log_memory.pb_text.h"
#include "tensorflow/core/framework/tensor_description.pb.h"
#include "tensorflow/core/framework/tensor_description.pb_text-impl.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_shape.pb_text-impl.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/types.pb_text-impl.h"
#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::MemoryLogStep& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::MemoryLogStep* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::MemoryLogTensorAllocation& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::MemoryLogTensorAllocation* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::MemoryLogTensorDeallocation& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::MemoryLogTensorDeallocation* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::MemoryLogTensorOutput& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::MemoryLogTensorOutput* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::MemoryLogRawAllocation& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::MemoryLogRawAllocation* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::MemoryLogRawDeallocation& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::MemoryLogRawDeallocation* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_framework_log_memory_proto_IMPL_H_

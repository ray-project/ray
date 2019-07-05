// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_util_event_proto_IMPL_H_
#define tensorflow_core_util_event_proto_IMPL_H_

#include "tensorflow/core/framework/resource_handle.pb.h"
#include "tensorflow/core/framework/resource_handle.pb_text-impl.h"
#include "tensorflow/core/framework/summary.pb.h"
#include "tensorflow/core/framework/summary.pb_text-impl.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/tensor.pb_text-impl.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_shape.pb_text-impl.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/types.pb_text-impl.h"
#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"
#include "tensorflow/core/util/event.pb.h"
#include "tensorflow/core/util/event.pb_text.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::Event& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::Event* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::LogMessage& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::LogMessage* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::SessionLog& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::SessionLog* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::TaggedRunMetadata& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::TaggedRunMetadata* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::WatchdogConfig& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::WatchdogConfig* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::WorkerHeartbeatRequest& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::WorkerHeartbeatRequest* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::WorkerHeartbeatResponse& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::WorkerHeartbeatResponse* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_util_event_proto_IMPL_H_

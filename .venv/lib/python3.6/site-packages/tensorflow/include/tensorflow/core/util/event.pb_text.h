// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_util_event_proto_H_
#define tensorflow_core_util_event_proto_H_

#include "tensorflow/core/util/event.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Enum text output for tensorflow.WorkerHealth
const char* EnumName_WorkerHealth(
    ::tensorflow::WorkerHealth value);

// Enum text output for tensorflow.WorkerShutdownMode
const char* EnumName_WorkerShutdownMode(
    ::tensorflow::WorkerShutdownMode value);

// Message-text conversion for tensorflow.Event
string ProtoDebugString(
    const ::tensorflow::Event& msg);
string ProtoShortDebugString(
    const ::tensorflow::Event& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::Event* msg)
        TF_MUST_USE_RESULT;

// Enum text output for tensorflow.LogMessage.Level
const char* EnumName_LogMessage_Level(
    ::tensorflow::LogMessage_Level value);

// Message-text conversion for tensorflow.LogMessage
string ProtoDebugString(
    const ::tensorflow::LogMessage& msg);
string ProtoShortDebugString(
    const ::tensorflow::LogMessage& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::LogMessage* msg)
        TF_MUST_USE_RESULT;

// Enum text output for tensorflow.SessionLog.SessionStatus
const char* EnumName_SessionLog_SessionStatus(
    ::tensorflow::SessionLog_SessionStatus value);

// Message-text conversion for tensorflow.SessionLog
string ProtoDebugString(
    const ::tensorflow::SessionLog& msg);
string ProtoShortDebugString(
    const ::tensorflow::SessionLog& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SessionLog* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.TaggedRunMetadata
string ProtoDebugString(
    const ::tensorflow::TaggedRunMetadata& msg);
string ProtoShortDebugString(
    const ::tensorflow::TaggedRunMetadata& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::TaggedRunMetadata* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.WatchdogConfig
string ProtoDebugString(
    const ::tensorflow::WatchdogConfig& msg);
string ProtoShortDebugString(
    const ::tensorflow::WatchdogConfig& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::WatchdogConfig* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.WorkerHeartbeatRequest
string ProtoDebugString(
    const ::tensorflow::WorkerHeartbeatRequest& msg);
string ProtoShortDebugString(
    const ::tensorflow::WorkerHeartbeatRequest& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::WorkerHeartbeatRequest* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.WorkerHeartbeatResponse
string ProtoDebugString(
    const ::tensorflow::WorkerHeartbeatResponse& msg);
string ProtoShortDebugString(
    const ::tensorflow::WorkerHeartbeatResponse& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::WorkerHeartbeatResponse* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_util_event_proto_H_

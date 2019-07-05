// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_step_stats_proto_H_
#define tensorflow_core_framework_step_stats_proto_H_

#include "tensorflow/core/framework/step_stats.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.AllocationRecord
string ProtoDebugString(
    const ::tensorflow::AllocationRecord& msg);
string ProtoShortDebugString(
    const ::tensorflow::AllocationRecord& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::AllocationRecord* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.AllocatorMemoryUsed
string ProtoDebugString(
    const ::tensorflow::AllocatorMemoryUsed& msg);
string ProtoShortDebugString(
    const ::tensorflow::AllocatorMemoryUsed& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::AllocatorMemoryUsed* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.NodeOutput
string ProtoDebugString(
    const ::tensorflow::NodeOutput& msg);
string ProtoShortDebugString(
    const ::tensorflow::NodeOutput& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::NodeOutput* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.MemoryStats
string ProtoDebugString(
    const ::tensorflow::MemoryStats& msg);
string ProtoShortDebugString(
    const ::tensorflow::MemoryStats& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::MemoryStats* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.NodeExecStats
string ProtoDebugString(
    const ::tensorflow::NodeExecStats& msg);
string ProtoShortDebugString(
    const ::tensorflow::NodeExecStats& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::NodeExecStats* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.DeviceStepStats
string ProtoDebugString(
    const ::tensorflow::DeviceStepStats& msg);
string ProtoShortDebugString(
    const ::tensorflow::DeviceStepStats& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::DeviceStepStats* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.StepStats
string ProtoDebugString(
    const ::tensorflow::StepStats& msg);
string ProtoShortDebugString(
    const ::tensorflow::StepStats& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::StepStats* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_step_stats_proto_H_

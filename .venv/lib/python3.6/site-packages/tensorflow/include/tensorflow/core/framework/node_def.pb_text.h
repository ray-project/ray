// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_node_def_proto_H_
#define tensorflow_core_framework_node_def_proto_H_

#include "tensorflow/core/framework/node_def.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.NodeDef.ExperimentalDebugInfo
string ProtoDebugString(
    const ::tensorflow::NodeDef_ExperimentalDebugInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::NodeDef_ExperimentalDebugInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::NodeDef_ExperimentalDebugInfo* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.NodeDef
string ProtoDebugString(
    const ::tensorflow::NodeDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::NodeDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::NodeDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_node_def_proto_H_

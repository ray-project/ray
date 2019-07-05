// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_cost_graph_proto_H_
#define tensorflow_core_framework_cost_graph_proto_H_

#include "tensorflow/core/framework/cost_graph.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.CostGraphDef.Node.InputInfo
string ProtoDebugString(
    const ::tensorflow::CostGraphDef_Node_InputInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::CostGraphDef_Node_InputInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::CostGraphDef_Node_InputInfo* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.CostGraphDef.Node.OutputInfo
string ProtoDebugString(
    const ::tensorflow::CostGraphDef_Node_OutputInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::CostGraphDef_Node_OutputInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::CostGraphDef_Node_OutputInfo* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.CostGraphDef.Node
string ProtoDebugString(
    const ::tensorflow::CostGraphDef_Node& msg);
string ProtoShortDebugString(
    const ::tensorflow::CostGraphDef_Node& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::CostGraphDef_Node* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.CostGraphDef
string ProtoDebugString(
    const ::tensorflow::CostGraphDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::CostGraphDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::CostGraphDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_cost_graph_proto_H_

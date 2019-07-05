// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_cost_graph_proto_IMPL_H_
#define tensorflow_core_framework_cost_graph_proto_IMPL_H_

#include "tensorflow/core/framework/cost_graph.pb.h"
#include "tensorflow/core/framework/cost_graph.pb_text.h"
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
    const ::tensorflow::CostGraphDef_Node_InputInfo& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::CostGraphDef_Node_InputInfo* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::CostGraphDef_Node_OutputInfo& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::CostGraphDef_Node_OutputInfo* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::CostGraphDef_Node& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::CostGraphDef_Node* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::CostGraphDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::CostGraphDef* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_framework_cost_graph_proto_IMPL_H_

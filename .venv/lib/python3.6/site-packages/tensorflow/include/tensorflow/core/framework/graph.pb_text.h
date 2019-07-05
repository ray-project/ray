// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_graph_proto_H_
#define tensorflow_core_framework_graph_proto_H_

#include "tensorflow/core/framework/graph.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.GraphDef
string ProtoDebugString(
    const ::tensorflow::GraphDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_graph_proto_H_

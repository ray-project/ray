// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_remote_fused_graph_execute_info_proto_H_
#define tensorflow_core_framework_remote_fused_graph_execute_info_proto_H_

#include "tensorflow/core/framework/remote_fused_graph_execute_info.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.RemoteFusedGraphExecuteInfo.TensorShapeTypeProto
string ProtoDebugString(
    const ::tensorflow::RemoteFusedGraphExecuteInfo_TensorShapeTypeProto& msg);
string ProtoShortDebugString(
    const ::tensorflow::RemoteFusedGraphExecuteInfo_TensorShapeTypeProto& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::RemoteFusedGraphExecuteInfo_TensorShapeTypeProto* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.RemoteFusedGraphExecuteInfo
string ProtoDebugString(
    const ::tensorflow::RemoteFusedGraphExecuteInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::RemoteFusedGraphExecuteInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::RemoteFusedGraphExecuteInfo* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_remote_fused_graph_execute_info_proto_H_

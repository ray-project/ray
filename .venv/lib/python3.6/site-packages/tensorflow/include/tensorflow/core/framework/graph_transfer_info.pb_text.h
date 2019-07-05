// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_graph_transfer_info_proto_H_
#define tensorflow_core_framework_graph_transfer_info_proto_H_

#include "tensorflow/core/framework/graph_transfer_info.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.GraphTransferNodeInput
string ProtoDebugString(
    const ::tensorflow::GraphTransferNodeInput& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphTransferNodeInput& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphTransferNodeInput* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.GraphTransferNodeInfo
string ProtoDebugString(
    const ::tensorflow::GraphTransferNodeInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphTransferNodeInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphTransferNodeInfo* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.GraphTransferConstNodeInfo
string ProtoDebugString(
    const ::tensorflow::GraphTransferConstNodeInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphTransferConstNodeInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphTransferConstNodeInfo* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.GraphTransferNodeInputInfo
string ProtoDebugString(
    const ::tensorflow::GraphTransferNodeInputInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphTransferNodeInputInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphTransferNodeInputInfo* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.GraphTransferNodeOutputInfo
string ProtoDebugString(
    const ::tensorflow::GraphTransferNodeOutputInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphTransferNodeOutputInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphTransferNodeOutputInfo* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.GraphTransferGraphInputNodeInfo
string ProtoDebugString(
    const ::tensorflow::GraphTransferGraphInputNodeInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphTransferGraphInputNodeInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphTransferGraphInputNodeInfo* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.GraphTransferGraphOutputNodeInfo
string ProtoDebugString(
    const ::tensorflow::GraphTransferGraphOutputNodeInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphTransferGraphOutputNodeInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphTransferGraphOutputNodeInfo* msg)
        TF_MUST_USE_RESULT;

// Enum text output for tensorflow.GraphTransferInfo.Destination
const char* EnumName_GraphTransferInfo_Destination(
    ::tensorflow::GraphTransferInfo_Destination value);

// Message-text conversion for tensorflow.GraphTransferInfo
string ProtoDebugString(
    const ::tensorflow::GraphTransferInfo& msg);
string ProtoShortDebugString(
    const ::tensorflow::GraphTransferInfo& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GraphTransferInfo* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_graph_transfer_info_proto_H_

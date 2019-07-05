// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_cluster_proto_H_
#define tensorflow_core_protobuf_cluster_proto_H_

#include "tensorflow/core/protobuf/cluster.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.JobDef
string ProtoDebugString(
    const ::tensorflow::JobDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::JobDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::JobDef* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.ClusterDef
string ProtoDebugString(
    const ::tensorflow::ClusterDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::ClusterDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ClusterDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_cluster_proto_H_

// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_cluster_proto_IMPL_H_
#define tensorflow_core_protobuf_cluster_proto_IMPL_H_

#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"
#include "tensorflow/core/protobuf/cluster.pb.h"
#include "tensorflow/core/protobuf/cluster.pb_text.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::JobDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::JobDef* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::ClusterDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::ClusterDef* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_cluster_proto_IMPL_H_

// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_config_proto_IMPL_H_
#define tensorflow_core_protobuf_config_proto_IMPL_H_

#include "tensorflow/core/framework/allocation_description.pb.h"
#include "tensorflow/core/framework/allocation_description.pb_text-impl.h"
#include "tensorflow/core/framework/attr_value.pb.h"
#include "tensorflow/core/framework/attr_value.pb_text-impl.h"
#include "tensorflow/core/framework/cost_graph.pb.h"
#include "tensorflow/core/framework/cost_graph.pb_text-impl.h"
#include "tensorflow/core/framework/function.pb.h"
#include "tensorflow/core/framework/function.pb_text-impl.h"
#include "tensorflow/core/framework/graph.pb.h"
#include "tensorflow/core/framework/graph.pb_text-impl.h"
#include "tensorflow/core/framework/node_def.pb.h"
#include "tensorflow/core/framework/node_def.pb_text-impl.h"
#include "tensorflow/core/framework/op_def.pb.h"
#include "tensorflow/core/framework/op_def.pb_text-impl.h"
#include "tensorflow/core/framework/resource_handle.pb.h"
#include "tensorflow/core/framework/resource_handle.pb_text-impl.h"
#include "tensorflow/core/framework/step_stats.pb.h"
#include "tensorflow/core/framework/step_stats.pb_text-impl.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/tensor.pb_text-impl.h"
#include "tensorflow/core/framework/tensor_description.pb.h"
#include "tensorflow/core/framework/tensor_description.pb_text-impl.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_shape.pb_text-impl.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/framework/types.pb_text-impl.h"
#include "tensorflow/core/framework/versions.pb.h"
#include "tensorflow/core/framework/versions.pb_text-impl.h"
#include "tensorflow/core/lib/strings/proto_text_util.h"
#include "tensorflow/core/lib/strings/scanner.h"
#include "tensorflow/core/protobuf/cluster.pb.h"
#include "tensorflow/core/protobuf/cluster.pb_text-impl.h"
#include "tensorflow/core/protobuf/config.pb.h"
#include "tensorflow/core/protobuf/config.pb_text.h"
#include "tensorflow/core/protobuf/debug.pb.h"
#include "tensorflow/core/protobuf/debug.pb_text-impl.h"
#include "tensorflow/core/protobuf/rewriter_config.pb.h"
#include "tensorflow/core/protobuf/rewriter_config.pb_text-impl.h"

namespace tensorflow {

namespace internal {

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::GPUOptions_Experimental_VirtualDevices& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::GPUOptions_Experimental_VirtualDevices* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::GPUOptions_Experimental& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::GPUOptions_Experimental* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::GPUOptions& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::GPUOptions* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OptimizerOptions& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OptimizerOptions* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::GraphOptions& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::GraphOptions* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::ThreadPoolOptionProto& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::ThreadPoolOptionProto* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::RPCOptions& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::RPCOptions* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::ConfigProto_Experimental& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::ConfigProto_Experimental* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::ConfigProto& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::ConfigProto* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::RunOptions_Experimental& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::RunOptions_Experimental* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::RunOptions& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::RunOptions* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::RunMetadata& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::RunMetadata* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::TensorConnection& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::TensorConnection* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::CallableOptions& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::CallableOptions* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_config_proto_IMPL_H_

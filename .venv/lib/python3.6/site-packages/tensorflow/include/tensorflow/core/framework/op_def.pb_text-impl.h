// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_op_def_proto_IMPL_H_
#define tensorflow_core_framework_op_def_proto_IMPL_H_

#include "tensorflow/core/framework/attr_value.pb.h"
#include "tensorflow/core/framework/attr_value.pb_text-impl.h"
#include "tensorflow/core/framework/op_def.pb.h"
#include "tensorflow/core/framework/op_def.pb_text.h"
#include "tensorflow/core/framework/resource_handle.pb.h"
#include "tensorflow/core/framework/resource_handle.pb_text-impl.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/tensor.pb_text-impl.h"
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
    const ::tensorflow::OpDef_ArgDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpDef_ArgDef* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpDef_AttrDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpDef_AttrDef* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpDef* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpDeprecation& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpDeprecation* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::OpList& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::OpList* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_framework_op_def_proto_IMPL_H_

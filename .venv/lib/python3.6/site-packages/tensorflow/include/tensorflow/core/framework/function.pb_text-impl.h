// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_function_proto_IMPL_H_
#define tensorflow_core_framework_function_proto_IMPL_H_

#include "tensorflow/core/framework/attr_value.pb.h"
#include "tensorflow/core/framework/attr_value.pb_text-impl.h"
#include "tensorflow/core/framework/function.pb.h"
#include "tensorflow/core/framework/function.pb_text.h"
#include "tensorflow/core/framework/node_def.pb.h"
#include "tensorflow/core/framework/node_def.pb_text-impl.h"
#include "tensorflow/core/framework/op_def.pb.h"
#include "tensorflow/core/framework/op_def.pb_text-impl.h"
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
    const ::tensorflow::FunctionDefLibrary& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::FunctionDefLibrary* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::FunctionDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::FunctionDef* msg);

void AppendProtoDebugString(
    ::tensorflow::strings::ProtoTextOutput* o,
    const ::tensorflow::GradientDef& msg);
bool ProtoParseFromScanner(
    ::tensorflow::strings::Scanner* scanner, bool nested, bool close_curly,
    ::tensorflow::GradientDef* msg);

}  // namespace internal

}  // namespace tensorflow

#endif  // tensorflow_core_framework_function_proto_IMPL_H_

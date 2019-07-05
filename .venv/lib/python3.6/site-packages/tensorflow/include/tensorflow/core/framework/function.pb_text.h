// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_function_proto_H_
#define tensorflow_core_framework_function_proto_H_

#include "tensorflow/core/framework/function.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.FunctionDefLibrary
string ProtoDebugString(
    const ::tensorflow::FunctionDefLibrary& msg);
string ProtoShortDebugString(
    const ::tensorflow::FunctionDefLibrary& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::FunctionDefLibrary* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.FunctionDef
string ProtoDebugString(
    const ::tensorflow::FunctionDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::FunctionDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::FunctionDef* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.GradientDef
string ProtoDebugString(
    const ::tensorflow::GradientDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::GradientDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::GradientDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_function_proto_H_

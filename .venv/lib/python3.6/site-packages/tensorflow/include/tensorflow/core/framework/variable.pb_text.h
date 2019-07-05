// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_variable_proto_H_
#define tensorflow_core_framework_variable_proto_H_

#include "tensorflow/core/framework/variable.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.VariableDef
string ProtoDebugString(
    const ::tensorflow::VariableDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::VariableDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::VariableDef* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.SaveSliceInfoDef
string ProtoDebugString(
    const ::tensorflow::SaveSliceInfoDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::SaveSliceInfoDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SaveSliceInfoDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_variable_proto_H_

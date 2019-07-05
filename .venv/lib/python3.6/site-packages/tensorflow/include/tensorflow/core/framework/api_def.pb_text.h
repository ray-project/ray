// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_api_def_proto_H_
#define tensorflow_core_framework_api_def_proto_H_

#include "tensorflow/core/framework/api_def.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Enum text output for tensorflow.ApiDef.Visibility
const char* EnumName_ApiDef_Visibility(
    ::tensorflow::ApiDef_Visibility value);

// Message-text conversion for tensorflow.ApiDef.Endpoint
string ProtoDebugString(
    const ::tensorflow::ApiDef_Endpoint& msg);
string ProtoShortDebugString(
    const ::tensorflow::ApiDef_Endpoint& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ApiDef_Endpoint* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.ApiDef.Arg
string ProtoDebugString(
    const ::tensorflow::ApiDef_Arg& msg);
string ProtoShortDebugString(
    const ::tensorflow::ApiDef_Arg& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ApiDef_Arg* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.ApiDef.Attr
string ProtoDebugString(
    const ::tensorflow::ApiDef_Attr& msg);
string ProtoShortDebugString(
    const ::tensorflow::ApiDef_Attr& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ApiDef_Attr* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.ApiDef
string ProtoDebugString(
    const ::tensorflow::ApiDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::ApiDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ApiDef* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.ApiDefs
string ProtoDebugString(
    const ::tensorflow::ApiDefs& msg);
string ProtoShortDebugString(
    const ::tensorflow::ApiDefs& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::ApiDefs* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_api_def_proto_H_

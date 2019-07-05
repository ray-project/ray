// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_op_def_proto_H_
#define tensorflow_core_framework_op_def_proto_H_

#include "tensorflow/core/framework/op_def.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.OpDef.ArgDef
string ProtoDebugString(
    const ::tensorflow::OpDef_ArgDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::OpDef_ArgDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpDef_ArgDef* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.OpDef.AttrDef
string ProtoDebugString(
    const ::tensorflow::OpDef_AttrDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::OpDef_AttrDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpDef_AttrDef* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.OpDef
string ProtoDebugString(
    const ::tensorflow::OpDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::OpDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpDef* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.OpDeprecation
string ProtoDebugString(
    const ::tensorflow::OpDeprecation& msg);
string ProtoShortDebugString(
    const ::tensorflow::OpDeprecation& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpDeprecation* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.OpList
string ProtoDebugString(
    const ::tensorflow::OpList& msg);
string ProtoShortDebugString(
    const ::tensorflow::OpList& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::OpList* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_op_def_proto_H_

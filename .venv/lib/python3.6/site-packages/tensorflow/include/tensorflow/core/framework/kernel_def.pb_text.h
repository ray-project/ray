// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_kernel_def_proto_H_
#define tensorflow_core_framework_kernel_def_proto_H_

#include "tensorflow/core/framework/kernel_def.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.KernelDef.AttrConstraint
string ProtoDebugString(
    const ::tensorflow::KernelDef_AttrConstraint& msg);
string ProtoShortDebugString(
    const ::tensorflow::KernelDef_AttrConstraint& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::KernelDef_AttrConstraint* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.KernelDef
string ProtoDebugString(
    const ::tensorflow::KernelDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::KernelDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::KernelDef* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.KernelList
string ProtoDebugString(
    const ::tensorflow::KernelList& msg);
string ProtoShortDebugString(
    const ::tensorflow::KernelList& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::KernelList* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_kernel_def_proto_H_

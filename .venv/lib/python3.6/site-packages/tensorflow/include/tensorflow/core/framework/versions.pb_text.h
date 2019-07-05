// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_versions_proto_H_
#define tensorflow_core_framework_versions_proto_H_

#include "tensorflow/core/framework/versions.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.VersionDef
string ProtoDebugString(
    const ::tensorflow::VersionDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::VersionDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::VersionDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_versions_proto_H_

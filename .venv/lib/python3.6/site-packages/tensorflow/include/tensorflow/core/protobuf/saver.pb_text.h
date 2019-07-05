// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_saver_proto_H_
#define tensorflow_core_protobuf_saver_proto_H_

#include "tensorflow/core/protobuf/saver.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Enum text output for tensorflow.SaverDef.CheckpointFormatVersion
const char* EnumName_SaverDef_CheckpointFormatVersion(
    ::tensorflow::SaverDef_CheckpointFormatVersion value);

// Message-text conversion for tensorflow.SaverDef
string ProtoDebugString(
    const ::tensorflow::SaverDef& msg);
string ProtoShortDebugString(
    const ::tensorflow::SaverDef& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SaverDef* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_saver_proto_H_

// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_protobuf_device_properties_proto_H_
#define tensorflow_core_protobuf_device_properties_proto_H_

#include "tensorflow/core/protobuf/device_properties.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.DeviceProperties
string ProtoDebugString(
    const ::tensorflow::DeviceProperties& msg);
string ProtoShortDebugString(
    const ::tensorflow::DeviceProperties& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::DeviceProperties* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.NamedDevice
string ProtoDebugString(
    const ::tensorflow::NamedDevice& msg);
string ProtoShortDebugString(
    const ::tensorflow::NamedDevice& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::NamedDevice* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_protobuf_device_properties_proto_H_

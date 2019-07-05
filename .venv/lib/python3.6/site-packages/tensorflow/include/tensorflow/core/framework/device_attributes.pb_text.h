// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_device_attributes_proto_H_
#define tensorflow_core_framework_device_attributes_proto_H_

#include "tensorflow/core/framework/device_attributes.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.InterconnectLink
string ProtoDebugString(
    const ::tensorflow::InterconnectLink& msg);
string ProtoShortDebugString(
    const ::tensorflow::InterconnectLink& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::InterconnectLink* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.LocalLinks
string ProtoDebugString(
    const ::tensorflow::LocalLinks& msg);
string ProtoShortDebugString(
    const ::tensorflow::LocalLinks& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::LocalLinks* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.DeviceLocality
string ProtoDebugString(
    const ::tensorflow::DeviceLocality& msg);
string ProtoShortDebugString(
    const ::tensorflow::DeviceLocality& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::DeviceLocality* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.DeviceAttributes
string ProtoDebugString(
    const ::tensorflow::DeviceAttributes& msg);
string ProtoShortDebugString(
    const ::tensorflow::DeviceAttributes& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::DeviceAttributes* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_device_attributes_proto_H_

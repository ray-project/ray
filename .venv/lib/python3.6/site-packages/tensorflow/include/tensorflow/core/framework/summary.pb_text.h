// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_summary_proto_H_
#define tensorflow_core_framework_summary_proto_H_

#include "tensorflow/core/framework/summary.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.SummaryDescription
string ProtoDebugString(
    const ::tensorflow::SummaryDescription& msg);
string ProtoShortDebugString(
    const ::tensorflow::SummaryDescription& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SummaryDescription* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.HistogramProto
string ProtoDebugString(
    const ::tensorflow::HistogramProto& msg);
string ProtoShortDebugString(
    const ::tensorflow::HistogramProto& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::HistogramProto* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.SummaryMetadata.PluginData
string ProtoDebugString(
    const ::tensorflow::SummaryMetadata_PluginData& msg);
string ProtoShortDebugString(
    const ::tensorflow::SummaryMetadata_PluginData& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SummaryMetadata_PluginData* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.SummaryMetadata
string ProtoDebugString(
    const ::tensorflow::SummaryMetadata& msg);
string ProtoShortDebugString(
    const ::tensorflow::SummaryMetadata& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::SummaryMetadata* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.Summary.Image
string ProtoDebugString(
    const ::tensorflow::Summary_Image& msg);
string ProtoShortDebugString(
    const ::tensorflow::Summary_Image& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::Summary_Image* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.Summary.Audio
string ProtoDebugString(
    const ::tensorflow::Summary_Audio& msg);
string ProtoShortDebugString(
    const ::tensorflow::Summary_Audio& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::Summary_Audio* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.Summary.Value
string ProtoDebugString(
    const ::tensorflow::Summary_Value& msg);
string ProtoShortDebugString(
    const ::tensorflow::Summary_Value& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::Summary_Value* msg)
        TF_MUST_USE_RESULT;

// Message-text conversion for tensorflow.Summary
string ProtoDebugString(
    const ::tensorflow::Summary& msg);
string ProtoShortDebugString(
    const ::tensorflow::Summary& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::Summary* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_summary_proto_H_

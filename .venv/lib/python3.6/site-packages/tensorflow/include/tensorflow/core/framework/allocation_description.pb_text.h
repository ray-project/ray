// GENERATED FILE - DO NOT MODIFY
#ifndef tensorflow_core_framework_allocation_description_proto_H_
#define tensorflow_core_framework_allocation_description_proto_H_

#include "tensorflow/core/framework/allocation_description.pb.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/protobuf.h"
#include "tensorflow/core/platform/types.h"

namespace tensorflow {

// Message-text conversion for tensorflow.AllocationDescription
string ProtoDebugString(
    const ::tensorflow::AllocationDescription& msg);
string ProtoShortDebugString(
    const ::tensorflow::AllocationDescription& msg);
bool ProtoParseFromString(
    const string& s,
    ::tensorflow::AllocationDescription* msg)
        TF_MUST_USE_RESULT;

}  // namespace tensorflow

#endif  // tensorflow_core_framework_allocation_description_proto_H_

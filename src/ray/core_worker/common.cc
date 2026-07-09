// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/common.h"

#include <memory>
#include <string>

#include "ray/util/process_utils.h"

namespace ray {
namespace core {

namespace {

std::shared_ptr<RayObject> MakeDirectTransportMetadataErrorObject(
    const std::string &error_message) {
  rpc::RayErrorInfo error_info;
  error_info.set_error_type(rpc::ErrorType::WORKER_STARTUP_FAILED);
  error_info.set_error_message(error_message);
  return std::make_shared<RayObject>(rpc::ErrorType::WORKER_STARTUP_FAILED, &error_info);
}

}  // namespace

std::string WorkerTypeString(WorkerType type) {
  // TODO(suquark): Use proto3 utils to get the string.
  if (type == WorkerType::DRIVER) {
    return "driver";
  } else if (type == WorkerType::WORKER) {
    return "worker";
  } else if (type == WorkerType::SPILL_WORKER) {
    return "spill_worker";
  } else if (type == WorkerType::RESTORE_WORKER) {
    return "restore_worker";
  }
  RAY_CHECK(false);
  return "";
}

std::string LanguageString(Language language) {
  if (language == Language::PYTHON) {
    return "python";
  } else if (language == Language::JAVA) {
    return "java";
  } else if (language == Language::CPP) {
    return "cpp";
  }
  RAY_CHECK(false);
  return "";
}

std::string GenerateCachedActorName(const std::string &ns,
                                    const std::string &actor_name) {
  return ns + "-" + actor_name;
}

void SerializeReturnObject(const ObjectID &object_id,
                           const std::shared_ptr<RayObject> &return_object,
                           rpc::ReturnObject *return_object_proto) {
  return_object_proto->set_object_id(object_id.Binary());

  if (!return_object) {
    // This should only happen if the local raylet died. Caller should
    // retry the task.
    RAY_LOG(WARNING).WithField(object_id)
        << "Failed to create task return object in the object store, exiting.";
    QuickExit();
  }

  auto object_to_serialize = return_object;
  return_object->WaitForDirectTransportMetadata();
  const auto direct_transport_metadata_error =
      return_object->GetDirectTransportMetadataError();
  if (direct_transport_metadata_error.has_value()) {
    object_to_serialize = MakeDirectTransportMetadataErrorObject(
        "Failed to extract RDT metadata asynchronously: " +
        *direct_transport_metadata_error);
  }

  return_object_proto->set_size(object_to_serialize->GetSize());
  if (object_to_serialize->GetData() != nullptr &&
      object_to_serialize->GetData()->IsPlasmaBuffer()) {
    return_object_proto->set_in_plasma(true);
  } else {
    if (object_to_serialize->GetData() != nullptr) {
      return_object_proto->set_data(object_to_serialize->GetData()->Data(),
                                    object_to_serialize->GetData()->Size());
    }
    if (object_to_serialize->GetMetadata() != nullptr) {
      return_object_proto->set_metadata(object_to_serialize->GetMetadata()->Data(),
                                        object_to_serialize->GetMetadata()->Size());
    }
  }
  for (const auto &nested_ref : object_to_serialize->GetNestedRefs()) {
    return_object_proto->add_nested_inlined_refs()->CopyFrom(nested_ref);
  }
  const auto direct_transport_metadata =
      object_to_serialize->GetDirectTransportMetadata();
  if (!direct_transport_metadata_error.has_value() &&
      direct_transport_metadata.has_value()) {
    return_object_proto->set_direct_transport_metadata(*direct_transport_metadata);
  }
}

}  // namespace core
}  // namespace ray

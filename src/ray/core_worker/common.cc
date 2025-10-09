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
#include <utility>

#include "ray/util/process.h"

namespace ray {
namespace core {

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
  return_object_proto->set_size(return_object->GetSize());
  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    return_object_proto->set_in_plasma(true);
  } else {
    if (return_object->GetData() != nullptr) {
      return_object_proto->set_data(return_object->GetData()->Data(),
                                    return_object->GetData()->Size());
    }
    if (return_object->GetMetadata() != nullptr) {
      return_object_proto->set_metadata(return_object->GetMetadata()->Data(),
                                        return_object->GetMetadata()->Size());
    }
  }
  for (const auto &nested_ref : return_object->GetNestedRefs()) {
    return_object_proto->add_nested_inlined_refs()->CopyFrom(nested_ref);
  }
}

}  // namespace core
}  // namespace ray

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

#include "ray/object_manager/plasma/create_request_queue.h"

#include <stdlib.h>

#include <memory>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/util/util.h"

namespace plasma {

uint64_t CreateRequestQueue::AddRequest(const ObjectID &object_id,
                                        const std::shared_ptr<ClientInterface> &client,
                                        const CreateObjectCallback &create_callback,
                                        size_t object_size) {
  auto req_id = next_req_id_++;
  fulfilled_requests_[req_id] = nullptr;
  queue_.emplace_back(
      new CreateRequest(object_id, req_id, client, create_callback, object_size));
  num_bytes_pending_ += object_size;
  return req_id;
}

bool CreateRequestQueue::GetRequestResult(uint64_t req_id,
                                          PlasmaObject *result,
                                          PlasmaError *error) {
  auto it = fulfilled_requests_.find(req_id);
  if (it == fulfilled_requests_.end()) {
    RAY_LOG(ERROR)
        << "Object store client requested the result of a previous request to create an "
           "object, but the result has already been returned to the client. This client "
           "may hang because the creation request cannot be fulfilled.";
    *error = PlasmaError::UnexpectedError;
    return true;
  }

  if (!it->second) {
    return false;
  }

  *result = it->second->result;
  *error = it->second->error;
  fulfilled_requests_.erase(it);
  return true;
}

std::pair<PlasmaObject, PlasmaError> CreateRequestQueue::TryRequestImmediately(
    const ObjectID &object_id,
    const std::shared_ptr<ClientInterface> &client,
    const CreateObjectCallback &create_callback,
    size_t object_size) {
  PlasmaObject result = {};

  // Immediately fulfill it using the fallback allocator.
  PlasmaError error = create_callback(/*fallback_allocator=*/true,
                                      &result,
                                      /*spilling_required=*/nullptr);
  return {result, error};
}

Status CreateRequestQueue::ProcessRequest(bool fallback_allocator,
                                          std::unique_ptr<CreateRequest> &request,
                                          bool *spilling_required) {
  request->error =
      request->create_callback(fallback_allocator, &request->result, spilling_required);
  if (request->error == PlasmaError::OutOfMemory) {
    return Status::ObjectStoreFull("");
  } else {
    return Status::OK();
  }
}

Status CreateRequestQueue::ProcessRequests() {
  // Suppress OOM dump to once per grace period.
  bool logged_oom = false;
  while (!queue_.empty()) {
    auto request_it = queue_.begin();
    bool spilling_required = false;
    auto status =
        ProcessRequest(/*fallback_allocator=*/false, *request_it, &spilling_required);
    if (spilling_required) {
      spill_objects_callback_();
    }
    auto now = get_time_();
    if (status.ok()) {
      FinishRequest(request_it);
      // Reset the oom start time since the creation succeeds.
      oom_start_time_ns_ = -1;
    } else {
      if (trigger_global_gc_) {
        trigger_global_gc_();
      }

      if (oom_start_time_ns_ == -1) {
        oom_start_time_ns_ = now;
      }
      auto grace_period_ns = oom_grace_period_ns_;
      auto spill_pending = spill_objects_callback_();
      if (spill_pending) {
        RAY_LOG(DEBUG) << "Reset grace period " << status << " " << spill_pending;
        oom_start_time_ns_ = -1;
        return Status::TransientObjectStoreFull("Waiting for objects to spill.");
      } else if (now - oom_start_time_ns_ < grace_period_ns) {
        // We need a grace period since (1) global GC takes a bit of time to
        // kick in, and (2) there is a race between spilling finishing and space
        // actually freeing up in the object store.
        RAY_LOG(DEBUG) << "In grace period before fallback allocation / oom.";
        return Status::ObjectStoreFull("Waiting for grace period.");
      } else {
        // Trigger the fallback allocator.
        status = ProcessRequest(/*fallback_allocator=*/true,
                                *request_it,
                                /*spilling_required=*/nullptr);
        if (!status.ok()) {
          std::string dump = "";
          if (dump_debug_info_callback_ && !logged_oom) {
            dump = dump_debug_info_callback_();
            logged_oom = true;
          }
          RAY_LOG(INFO) << "Out-of-memory: Failed to create object "
                        << (*request_it)->object_id << " of size "
                        << (*request_it)->object_size / 1024 / 1024 << "MB\n"
                        << dump;
        }
        FinishRequest(request_it);
      }
    }
  }
  return Status::OK();
}

void CreateRequestQueue::FinishRequest(
    std::list<std::unique_ptr<CreateRequest>>::iterator request_it) {
  // Fulfill the request.
  auto &request = *request_it;
  auto it = fulfilled_requests_.find(request->request_id);
  RAY_CHECK(it != fulfilled_requests_.end());
  RAY_CHECK(it->second == nullptr);
  it->second = std::move(request);
  RAY_CHECK(num_bytes_pending_ >= it->second->object_size);
  num_bytes_pending_ -= it->second->object_size;
  queue_.erase(request_it);
}

void CreateRequestQueue::RemoveDisconnectedClientRequests(
    const std::shared_ptr<ClientInterface> &client) {
  for (auto it = queue_.begin(); it != queue_.end();) {
    if ((*it)->client == client) {
      fulfilled_requests_.erase((*it)->request_id);
      RAY_CHECK(num_bytes_pending_ >= (*it)->object_size);
      num_bytes_pending_ -= (*it)->object_size;
      it = queue_.erase(it);
    } else {
      it++;
    }
  }

  for (auto it = fulfilled_requests_.begin(); it != fulfilled_requests_.end();) {
    if (it->second && it->second->client == client) {
      fulfilled_requests_.erase(it++);
    } else {
      it++;
    }
  }
}

}  // namespace plasma

#include "ray/object_manager/plasma/get_request_queue.h"

namespace plasma {

GetRequest::GetRequest(
    instrumented_io_context &io_context, const std::shared_ptr<ClientInterface> &client,
    const std::vector<ObjectID> &object_ids, bool is_from_worker,
    std::function<void(const std::shared_ptr<GetRequest> &get_req)> &callback)
    : client(client),
      object_ids(object_ids.begin(), object_ids.end()),
      objects(object_ids.size()),
      num_satisfied(0),
      is_from_worker(is_from_worker),
      callback(callback),
      timer_(io_context) {
  std::unordered_set<ObjectID> unique_ids(object_ids.begin(), object_ids.end());
  num_objects_to_wait_for = unique_ids.size();
};

void GetRequestQueue::AddRequest(const std::shared_ptr<ClientInterface> &client,
                                 const std::vector<ObjectID> &object_ids,
                                 int64_t timeout_ms, bool is_from_worker,
                                 ObjectReadyCallback callback) {
  // Create a get request for this object.
  auto get_req = std::make_shared<GetRequest>(
      GetRequest(io_context_, client, object_ids, is_from_worker, callback));
  for (auto object_id : object_ids) {
    // Check if this object is already present
    // locally. If so, record that the object is being used and mark it as accounted for.
    auto entry = object_lifecycle_mgr_->GetObject(object_id);
    if (entry && entry->Sealed()) {
      // Update the get request to take into account the present object.
      ToPlasmaObject(*entry, &get_req->objects[object_id], /* checksealed */ true);
      get_req->num_satisfied += 1;
    } else {
      // Add a placeholder plasma object to the get request to indicate that the
      // object is not present. This will be parsed by the client. We set the
      // data size to -1 to indicate that the object is not present.
      get_req->objects[object_id].data_size = -1;
      // Add the get request to the relevant data structures.
      object_get_requests_[object_id].push_back(get_req);
    }
  }

  // If all of the objects are present already or if the timeout is 0, return to
  // the client.
  if (get_req->num_satisfied == get_req->num_objects_to_wait_for || timeout_ms == 0) {
    callback(get_req);
    // Remove the get request from each of the relevant object_get_requests hash
    // tables if it is present there. It should only be present there if the get
    // request timed out.
    RemoveGetRequest(get_req);
  } else if (timeout_ms != -1) {
    // Set a timer that will cause the get request to return to the client. Note
    // that a timeout of -1 is used to indicate that no timer should be set.
    get_req->AsyncWait(timeout_ms,
                       [get_req, callback](const boost::system::error_code &ec) {
                         if (ec != boost::asio::error::operation_aborted) {
                           // Timer was not cancelled, take necessary action.
                           callback(get_req);
                         }
                       });
  }
}

void GetRequestQueue::RemoveGetRequestsForClient(
    const std::shared_ptr<ClientInterface> &client) {
  std::unordered_set<std::shared_ptr<GetRequest>> get_requests_to_remove;
  for (auto const &pair : object_get_requests_) {
    for (const auto &get_request : pair.second) {
      if (get_request->client == client) {
        get_requests_to_remove.insert(get_request);
      }
    }
  }

  // It shouldn't be possible for a given client to be in the middle of multiple get
  // requests.
  RAY_CHECK(get_requests_to_remove.size() <= 1);
  for (const auto &get_request : get_requests_to_remove) {
    RemoveGetRequest(get_request);
  }
}

void GetRequestQueue::RemoveGetRequest(const std::shared_ptr<GetRequest> &get_request) {
  // Remove the get request from each of the relevant object_get_requests hash
  // tables if it is present there. It should only be present there if the get
  // request timed out or if it was issued by a client that has disconnected.
  for (ObjectID &object_id : get_request->object_ids) {
    auto object_request_iter = object_get_requests_.find(object_id);
    if (object_request_iter != object_get_requests_.end()) {
      auto &get_requests = object_request_iter->second;
      // Erase get_req from the vector.
      auto it = std::find(get_requests.begin(), get_requests.end(), get_request);
      if (it != get_requests.end()) {
        get_requests.erase(it);
        // If the vector is empty, remove the object ID from the map.
        if (get_requests.empty()) {
          object_get_requests_.erase(object_request_iter);
        }
      }
    }
  }
  // Remove the get request.
  get_request->CancelTimer();
  get_request->MarkRemoved();
}

void GetRequestQueue::ObjectSealed(const ObjectID &object_id) {
  auto it = object_get_requests_.find(object_id);
  // If there are no get requests involving this object, then return.
  if (it == object_get_requests_.end()) {
    return;
  }

  auto &get_requests = it->second;

  // After finishing the loop below, get_requests and it will have been
  // invalidated by the removal of object_id from object_get_requests_.
  size_t index = 0;
  size_t num_requests = get_requests.size();
  for (size_t i = 0; i < num_requests; ++i) {
    auto get_req = get_requests[index];
    auto entry = object_lifecycle_mgr_->GetObject(object_id);
    RAY_CHECK(entry != nullptr);
    ToPlasmaObject(*entry, &get_req->objects[object_id], /* check sealed */ true);
    get_req->num_satisfied += 1;
    // If this get request is done, reply to the client.
    if (get_req->num_satisfied == get_req->num_objects_to_wait_for) {
      get_req->callback(get_req);
      RemoveGetRequest(get_req);
    } else {
      // The call to ReturnFromGet will remove the current element in the
      // array, so we only increment the counter in the else branch.
      index += 1;
    }
  }

  // No get requests should be waiting for this object anymore. The object ID
  // may have been removed from the object_get_requests_ by ReturnFromGet, but
  // if the get request has not returned yet, then remove the object ID from the
  // map here.
  it = object_get_requests_.find(object_id);
  if (it != object_get_requests_.end()) {
    object_get_requests_.erase(object_id);
  }
}

bool GetRequestQueue::IsGetRequestExist(const ObjectID &object_id) {
  return object_get_requests_.count(object_id) > 0;
}
}  // namespace plasma
// Copyright 2020-2021 The Ray Authors.
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

#pragma once

#include <ray/api/wait_result.h>

#include <memory>
#include <msgpack.hpp>

#include "ray/common/id.h"

namespace ray {
namespace internal {

class ObjectStore {
 public:
  /// The default timeout to get object.
  static const int default_get_timeout_ms = 1000;

  virtual ~ObjectStore(){};

  /// Store an object in the object store.
  ///
  /// \param[in] data The serialized object data buffer to store.
  /// \param[out] The id which is allocated to the object.
  void Put(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id);

  /// Store an object in the object store.
  ///
  /// \param[in] data The serialized object data buffer to store.
  /// \param[in] object_id The object which should be stored.
  void Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id);

  /// Get a single object from the object store.
  /// This method will be blocked until the object are ready or wait for timeout.
  ///
  /// \param[in] object_id The object id which should be got.
  /// \param[in] timeout_ms The maximum wait time in milliseconds.
  /// \return shared pointer of the result buffer.
  std::shared_ptr<msgpack::sbuffer> Get(const ObjectID &object_id,
                                        int timeout_ms = default_get_timeout_ms);

  /// Get a list of objects from the object store.
  /// This method will be blocked until all the objects are ready or wait for timeout.
  ///
  /// \param[in] ids The object id array which should be got.
  /// \param[in] timeout_ms The maximum wait time in milliseconds.
  /// \return shared pointer array of the result buffer.
  std::vector<std::shared_ptr<msgpack::sbuffer>> Get(
      const std::vector<ObjectID> &ids, int timeout_ms = default_get_timeout_ms);

  /// Wait for a list of ObjectRefs to be locally available,
  /// until specified number of objects are ready, or specified timeout has passed.
  ///
  /// \param[in] ids The object id array which should be waited.
  /// \param[in] num_objects The minimum number of objects to wait.
  /// \param[in] timeout_ms The maximum wait time in milliseconds.
  /// \return A vector that indicates each object has appeared or not.
  virtual std::vector<bool> Wait(const std::vector<ObjectID> &ids,
                                 int num_objects,
                                 int timeout_ms) = 0;

  /// Increase the reference count for this object ID.
  /// Increase the local reference count for this object ID. Should be called
  /// by the language frontend when a new reference is created.
  ///
  /// \param[in] id The binary string ID to increase the reference count for.
  virtual void AddLocalReference(const std::string &id) = 0;

  /// Decrease the reference count for this object ID. Should be called
  /// by the language frontend when a reference is destroyed.
  ///
  /// \param[in] id The binary string ID to decrease the reference count for.
  virtual void RemoveLocalReference(const std::string &id) = 0;

  /// Returns a map of all ObjectIDs currently in scope with a pair of their
  /// (local, submitted_task) reference counts. For debugging purposes.
  std::unordered_map<ObjectID, std::pair<size_t, size_t>> GetAllReferenceCounts() const;

 private:
  virtual void PutRaw(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id) = 0;

  virtual void PutRaw(std::shared_ptr<msgpack::sbuffer> data,
                      const ObjectID &object_id) = 0;

  virtual std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &object_id,
                                                   int timeout_ms) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(
      const std::vector<ObjectID> &ids, int timeout_ms) = 0;
};
}  // namespace internal
}  // namespace ray
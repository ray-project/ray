
#pragma once

#include <ray/api/wait_result.h>

#include <memory>
#include <msgpack.hpp>

namespace ray {
namespace api {

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
  /// \return WaitResult Two arrays, one containing locally available objects, one
  /// containing the rest.
  virtual WaitResult Wait(const std::vector<ObjectID> &ids, int num_objects,
                          int timeout_ms) = 0;

 private:
  virtual void PutRaw(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id) = 0;

  virtual void PutRaw(std::shared_ptr<msgpack::sbuffer> data,
                      const ObjectID &object_id) = 0;

  virtual std::shared_ptr<msgpack::sbuffer> GetRaw(const ObjectID &object_id,
                                                   int timeout_ms) = 0;

  virtual std::vector<std::shared_ptr<msgpack::sbuffer>> GetRaw(
      const std::vector<ObjectID> &ids, int timeout_ms) = 0;
};
}  // namespace api
}  // namespace ray
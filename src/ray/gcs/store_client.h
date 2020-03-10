#ifndef RAY_GCS_STORE_CLIENT_H
#define RAY_GCS_STORE_CLIENT_H

#include <string>
#include "ray/gcs/callback.h"

namespace ray {

namespace gcs {

class StoreClient {
 public:
  virtual ~StoreClient() {}

  /// Connect to storage. Non-thread safe.
  ///
  /// \return Status
  virtual Status Connect(boost::asio::io_service &io_service) = 0;

  /// Disconnect with storage. Non-thread safe.
  virtual void Disconnect() = 0;

  /// Write data to storage asynchronously.
  ///
  /// \param table_name
  /// \param key
  /// \param value
  /// \param callback Callback that will be called after write finishes.
  /// \return Status
  virtual Status AsyncPut(const std::string &table_name, const std::string &key,
                          const std::string &value, const StatusCallback &callback) = 0;

  virtual Status AsyncPut(const std::string &table_name, const std::string &key,
                          const std::string &index, const std::string &value,
                          const StatusCallback &callback) = 0;

  virtual Status AsyncGet(const std::string &table_name, const std::string &key,
                          const OptionalItemCallback<std::string> &callback) = 0;

  virtual Status AsyncGetByIndex(const std::string &table_name, const std::string &index,
                                 const MultiItemCallback &callback) = 0;

  virtual Status AsyncGetAll(const std::string &table_name,
                             const ScanCallback<std::string> &callback) = 0;

  virtual Status AsyncDelete(const std::string &table_name, const std::string &key,
                             const StatusCallback &callback) = 0;

  virtual Status AsyncDeleteByIndex(const std::string &table_name,
                                    const std::string &index,
                                    const StatusCallback &callback) = 0;

 protected:
  StoreClient() {}
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_H

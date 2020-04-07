#ifndef RAY_GCS_STORE_CLIENT_STORE_TABLES_H
#define RAY_GCS_STORE_CLIENT_STORE_TABLES_H

#include "ray/common/id.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {

namespace gcs {

class GcsServerInfoTable {
 public:
  GcsServerInfoTable(std::shared_ptr<GcsServerInfoTableImpl> table_impl);

  ~GcsServerInfoTable();

  Status AsyncPut(const GcsServerID &server_id, const rpc::GcsServerInfo &server_info,
                  const StatusCallback &callback);

  Status AsyncGet(const GcsServerID &server_id,
                  const OptionalItemCallback<rpc::GcsServerInfo> &callback);

 private:
  std::string table_name_;

  std::shared_ptr<GcsServerInfoTableImpl> table_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_STORE_TABLES_H
#include "ray/gcs/store_client/store_tables.h"
#include "ray/common/constants.h"

namespace ray {

namespace gcs {

GcsServerInfoTable::GcsServerInfoTable(std::shared_ptr<GcsServerInfoTableImpl> table_impl)
    : table_impl_(std::move(table_impl)) {
  // TODO(micafan) Table name should contain ray cluster name.
  // Ensure that different ray clusters do not overwrite each other when sharing storage.
  table_name_ = kGcsServerInfoTableNamePrefix;
}

GcsServerInfoTable::~GcsServerInfoTable() {}

Status GcsServerInfoTable::AsyncPut(const GcsServerID &server_id,
                                    const rpc::GcsServerInfo &server_info,
                                    const StatusCallback &callback) {
  return table_impl_->AsyncPut(table_name_, server_id, server_info, callback);
}

Status GcsServerInfoTable::AsyncGet(
    const GcsServerID &server_id,
    const OptionalItemCallback<rpc::GcsServerInfo> &callback) {
  return table_impl_->AsyncGet(table_name_, server_id, callback);
}

}  // namespace gcs

}  // namespace ray
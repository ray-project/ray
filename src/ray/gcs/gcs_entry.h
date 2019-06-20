#ifndef RAY_GCS_GCS_ENTRY_H
#define RAY_GCS_GCS_ENTRY_H

#include "ray/protobuf/gcs.pb.h"
#include "ray/rpc/message_wrapper.h"
#include "ray/rpc/util.h"

namespace ray {

namespace gcs {

template <class ID, class Entry>
class GcsEntry : public rpc::ConstMessageWrapper<rpc::GcsEntry> {
 public:
  explicit GcsEntry(const rpc::GcsEntry &message) : ConstMessageWrapper(message) {}

  explicit GcsEntry(const std::string &data)
      : ConstMessageWrapper(ParseGcsEntryMessage(data)) {}

  GcsEntry(const ID &id, const rpc::GcsChangeMode &change_mode,
           const std::vector<Entry> &entries)
      : ConstMessageWrapper(CreateGcsEntryMessage(id, change_mode, entries)) {}

  const ID GetId() { return ID::FromBinary(message_->id()); }

  const rpc::GcsChangeMode GetChangeMode() { return message_->change_mode(); }

  const std::vector<Entry> GetEntries() {
//    return rpc::VectorFromProtobuf<Entry>(message_->entries());
// XXX
      return {};
  }

 private:
  inline static std::unique_ptr<const rpc::GcsEntry> ParseGcsEntryMessage(
      const std::string &data) {
    auto *gcs_entry = new rpc::GcsEntry();
    gcs_entry->ParseFromString(data);
    return std::unique_ptr<const rpc::GcsEntry>(gcs_entry);
  }

  static inline std::unique_ptr<const rpc::GcsEntry> CreateGcsEntryMessage(
      const ID &id, const rpc::GcsChangeMode &change_mode,
      const std::vector<Entry> &entries) {
    auto *gcs_entry = new rpc::GcsEntry();
    gcs_entry->set_id(id.ToBinary());
    gcs_entry->set_change_mode(change_mode);
    for (const auto &entry : entries) {
      std::string str;
      entry.SerializeToString(&str);
      gcs_entry->add_entries(std::move(str));
    }
    return std::unique_ptr<const rpc::GcsEntry>(gcs_entry);
  }
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_ENTRY_H

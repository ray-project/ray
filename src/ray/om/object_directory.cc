#include "object_directory.h"

namespace ray {

  // TODO(hme): Implement using GCS for object lookup.
  ObjectDirectory::ObjectDirectory(){

  }

  UniqueID ObjectDirectory::GetLocations(const ObjectID &object,
                                         const Callback &callback) {
    return UniqueID().from_random();
  };

  Status ObjectDirectory::Cancel(const UniqueID &callback_id) {
    return Status::OK();
  };

} // namespace ray

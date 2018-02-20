#include "object_directory.h"

namespace ray {

// TODO(hme): Implement using GCS for object lookup.
class ObjectDirectory : public ObjectDirectoryInterface {

 public:
  ObjectDirectory(){}

  GetLocationsID GetLocations(const ObjectID &object,
                              const void *callback) override {
    GetLocationsID id = GetLocationsID();
    return id;
  };

  Status Cancel(const GetLocationsID &getloc_id) override {
    Status status = Status();
    return status;
  };

};

} // namespace ray

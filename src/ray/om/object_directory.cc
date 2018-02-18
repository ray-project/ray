#include "object_directory.h"

// TODO(hme): Implement using GCS for object lookup.
class ObjectDirectory : public ObjectDirectoryInterface {

 public:
  ObjectDirectory(){}

  ray::GetLocationsID GetLocations(const ray::ObjectID &object,
                                   const void *callback) override {
    ray::GetLocationsID id = ray::GetLocationsID();
    return id;
  };

  ray::Status Cancel(const ray::GetLocationsID &getloc_id) override {
    ray::Status status = ray::Status();
    return status;
  };

};

#ifndef RAY_OBJECTDIRECTORY_H
#define RAY_OBJECTDIRECTORY_H

#include "vector"

#include "ray/id.h"
#include "ray/status.h"

namespace ray {

typedef ray::UniqueID GetLocationsID;

class ObjectDirectoryInterface {
 public:
  ObjectDirectoryInterface(){}

  // Asynchronously obtain the locations of an object by ObjectID.
  // Returns GetLocationsID, which uniquely identifies an invocation of
  // this method. The callback should implement the following signature:
  //   f(plasma::Status status, const std::vector<DBClientID> &db_client_ids)
  //
  // Status passed to the callback indicates the outcome of invoking this
  // method.
  virtual GetLocationsID GetLocations(const ObjectID &object,
                                      const void *callback)=0;

  // Cancels the invocation of the callback associated with GetLocationsID.
  virtual Status Cancel(const GetLocationsID &getloc_id)=0;
};

class ObjectDirectory;

} // namespace ray

#endif // RAY_OBJECTDIRECTORY_H

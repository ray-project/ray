#ifndef RAY_GCS_ACTOR_STATE_ACCESSOR_H
#define RAY_GCS_ACTOR_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/call_back.h"
#include "ray/gcs/format/gcs_generated.h"

namespace ray {

namespace gcs {

class AsyncGcsClient;

class ActorStateAccessor {
 public:
  ActorStateAccessor(AsyncGcsClient *client_impl);

  ~ActorStateAccessor() {}

  Status AsyncGet(const DriverID &driver_id, const ActorID &actor_id,
                  DatumCallback<ActorTableDataT>::SingleItem callback);

  Status AsyncGetCheckpointIds(
      const DriverID &driver_id, const ActorID &actor_id,
      DatumCallback<ActorCheckpointIdDataT>::SingleItem callback);

 private:
  AsyncGcsClient *client_impl_{nullptr};
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ACTOR_STATE_ACCESSOR_H

#ifndef RAY_GCS_ACTOR_STATE_ACCESSOR_H
#define RAY_GCS_ACTOR_STATE_ACCESSOR_H

#include "ray/common/id.h"
#include "ray/gcs/call_back.h"
#include "ray/gcs/tables.h"

namespace ray {

namespace gcs {

class GcsClientImpl;

class ActorStateAccessor {
 public:
  ActorStateAccessor(GcsClientImpl &client_impl);

  ~ActorStateAccessor() {}

  Status AsyncGet(const DriverID &driver_id, const ActorID &actor_id,
                  const DatumCallback<ActorTableData>::OptionalItem &callback);

  Status AsyncAdd(const DriverID &driver_id, const ActorID &actor_id,
                  std::shared_ptr<ActorTableData> data_ptr, size_t log_length,
                  const StatusCallback &callback);

  Status AsyncSubscribe(const DriverID &driver_id,
                        const DatumCallback<ActorTableData>::MultiItem &subscribe,
                        const StatusCallback &done);

  Status AsyncGetCheckpointIds(
      const DriverID &driver_id, const ActorID &actor_id,
      const DatumCallback<ActorCheckpointIdData>::OptionalItem &callback);

 private:
  GcsClientImpl &client_impl_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_ACTOR_STATE_ACCESSOR_H

#ifndef RAY_RAYLET_ACTOR_REGISTRATION_H
#define RAY_RAYLET_ACTOR_REGISTRATION_H

#include <unordered_map>

#include "ray/gcs/format/gcs_generated.h"
#include "ray/id.h"

namespace ray {

namespace raylet {

class ActorRegistration {
 public:
  ActorRegistration(const ActorTableDataT &actor_table_data);

  const ClientID GetNodeManagerId() const;

  void RegisterHandle(const ActorHandleID &handle_id);

  void RegisterTask(const ActorHandleID &handle_id, const ObjectID &execution_dependency);

 private:
  ActorTableDataT actor_table_data_;
  ObjectID execution_dependency_;
  std::unordered_map<ActorHandleID, int64_t, UniqueIDHasher> frontier_task_counts_;
  std::unordered_map<ActorHandleID, ObjectID, UniqueIDHasher> frontier_dependencies;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_ACTOR_REGISTRATION_H

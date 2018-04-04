#include "ray/raylet/actor_registration.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ActorRegistration::ActorRegistration(const ActorTableDataT &actor_table_data)
    : actor_table_data_(actor_table_data), execution_dependency_(ObjectID::nil()) {}

const ClientID ActorRegistration::GetNodeManagerId() const {
  return ClientID::from_binary(actor_table_data_.node_manager_id);
}

void ActorRegistration::RegisterHandle(const ActorHandleID &handle_id) {
  throw std::runtime_error("Method not implemented");
}

void ActorRegistration::RegisterTask(const ActorHandleID &handle_id,
                                     const ObjectID &execution_dependency) {
  throw std::runtime_error("Method not implemented");
}

}  // namespace raylet

}  // namespace ray

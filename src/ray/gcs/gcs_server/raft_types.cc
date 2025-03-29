#include "ray/gcs/gcs_server/raft_types.h"

#include <string>

#include "ray/protobuf/gcs_tables.pb.h"
#include "ray/protobuf/raft.pb.h"

namespace ray {
namespace gcs {

void StateUpdate::ApplyTo(ConsensusState &state) const {
  switch (type_) {
    case Type::RESOURCE_UPDATE: {
      ResourceView view;
      view.ParseFromString(data_);
      state.SetResourceView(view);
      break;
    }
    case Type::ACTOR_UPDATE: {
      ActorTable table;
      table.ParseFromString(data_);
      state.SetActorTable(table);
      break;
    }
    case Type::TASK_UPDATE: {
      TaskTable table;
      table.ParseFromString(data_);
      state.SetTaskTable(table);
      break;
    }
    case Type::NODE_UPDATE: {
      NodeTable table;
      table.ParseFromString(data_);
      state.SetNodeTable(table);
      break;
    }
    case Type::PLACEMENT_GROUP_UPDATE: {
      PlacementGroupTable table;
      table.ParseFromString(data_);
      state.SetPlacementGroupTable(table);
      break;
    }
  }
}

bool ConsensusState::SerializeToString(std::string *output) const {
  ConsensusStateProto proto;
  proto.set_current_term(current_term_);
  proto.set_leader_id(leader_id_);
  proto.mutable_resource_view()->CopyFrom(resource_view_);
  proto.mutable_actor_table()->CopyFrom(actor_table_);
  proto.mutable_task_table()->CopyFrom(task_table_);
  proto.mutable_node_table()->CopyFrom(node_table_);
  proto.mutable_placement_group_table()->CopyFrom(placement_group_table_);
  return proto.SerializeToString(output);
}

bool ConsensusState::ParseFromString(const std::string &input) {
  ConsensusStateProto proto;
  if (!proto.ParseFromString(input)) {
    return false;
  }
  current_term_ = proto.current_term();
  leader_id_ = proto.leader_id();
  resource_view_.CopyFrom(proto.resource_view());
  actor_table_.CopyFrom(proto.actor_table());
  task_table_.CopyFrom(proto.task_table());
  node_table_.CopyFrom(proto.node_table());
  placement_group_table_.CopyFrom(proto.placement_group_table());
  return true;
}

}  // namespace gcs
}  // namespace ray 
#include "ray/gcs/pb_util.h"

namespace ray {
namespace core {

inline std::optional<rpc::RayErrorInfo> GetErrorInfoFromGetTaskFailureCauseReply(
    const rpc::WorkerAddress &addr,
    const Status &get_task_failure_cause_reply_status,
    const rpc::GetTaskFailureCauseReply &get_task_failure_cause_reply) {
  if (get_task_failure_cause_reply_status.ok()) {
    RAY_LOG(DEBUG) << "Task failure cause for worker " << addr.worker_id << ": "
                   << ray::gcs::RayErrorInfoToString(
                          get_task_failure_cause_reply.failure_cause());
    if (get_task_failure_cause_reply.has_failure_cause()) {
      return std::make_optional(get_task_failure_cause_reply.failure_cause());
    } else {
      return std::nullopt;
    }
  } else {
    RAY_LOG(DEBUG) << "Failed to fetch task result with status "
                   << get_task_failure_cause_reply_status.ToString()
                   << " node id: " << addr.raylet_id << " ip: " << addr.ip_address;
    std::stringstream buffer;
    buffer << "Task failed due to the node dying.\n\nThe node (IP: " << addr.ip_address
           << ", node ID: " << addr.raylet_id
           << ") where this task was running crashed unexpectedly. "
           << "This can happen if: (1) the instance where the node was running failed, "
              "(2) raylet crashes unexpectedly (OOM, preempted node, etc).\n\n"
           << "To see more information about the crash, use `ray logs raylet.out -ip "
           << addr.ip_address << "`";
    rpc::RayErrorInfo error_info;
    error_info.set_error_message(buffer.str());
    error_info.set_error_type(rpc::ErrorType::NODE_DIED);
    return std::make_optional(error_info);
  }
}

}  // namespace core
}  // namespace ray

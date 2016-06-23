#include "computation_graph.h"

OperationId ComputationGraph::add_operation(std::unique_ptr<Operation> operation) {
  OperationId operationid = operations_.size();
  OperationId creator_operationid = operation->creator_operationid();
  RAY_CHECK_EQ(spawned_operations_.size(), operationid, "ComputationGraph is attempting to call add_operation, but spawned_operations_.size() != operationid.");
  operations_.emplace_back(std::move(operation));
  if (creator_operationid != NO_OPERATION && creator_operationid != ROOT_OPERATION) {
    spawned_operations_[creator_operationid].push_back(operationid);
  }
  spawned_operations_.push_back(std::vector<OperationId>());
  return operationid;
}

const Task& ComputationGraph::get_task(OperationId operationid) {
  RAY_CHECK_NEQ(operationid, ROOT_OPERATION, "ComputationGraph attempting to get_task with operationid == ROOT_OPERATION");
  RAY_CHECK_NEQ(operationid, NO_OPERATION, "ComputationGraph attempting to get_task with operationid == NO_OPERATION");
  RAY_CHECK_LT(operationid, operations_.size(), "ComputationGraph attempting to get_task with operationid " << operationid << ", but operationid >= operations_.size().");
  RAY_CHECK(operations_[operationid]->has_task(), "Calling get_task with operationid " << operationid << ", but this corresponds to a put not a task.");
  return operations_[operationid]->task();
}

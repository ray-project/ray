package org.ray.streaming.schedule.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.streaming.core.graph.ExecutionEdge;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.graph.ExecutionNode;
import org.ray.streaming.core.graph.ExecutionTask;
import org.ray.streaming.core.processor.ProcessBuilder;
import org.ray.streaming.core.processor.StreamProcessor;
import org.ray.streaming.core.runtime.StreamWorker;
import org.ray.streaming.plan.Plan;
import org.ray.streaming.plan.PlanEdge;
import org.ray.streaming.plan.PlanVertex;
import org.ray.streaming.schedule.ITaskAssign;

public class TaskAssignImpl implements ITaskAssign {

  /**
   * Assign an optimized logical plan to execution graph.
   *
   * @param plan The logical plan.
   * @param workers The worker actors.
   * @return The physical execution graph.
   */
  @Override
  public ExecutionGraph assign(Plan plan, List<RayActor<StreamWorker>> workers) {
    List<PlanVertex> planVertices = plan.getPlanVertexList();
    List<PlanEdge> planEdges = plan.getPlanEdgeList();

    int taskId = 0;
    Map<Integer, ExecutionNode> idToExecutionNode = new HashMap<>();
    for (PlanVertex planVertex : planVertices) {
      ExecutionNode executionNode = new ExecutionNode(planVertex.getVertexId(),
          planVertex.getParallelism());
      executionNode.setNodeType(planVertex.getVertexType());
      List<ExecutionTask> vertexTasks = new ArrayList<>();
      for (int taskIndex = 0; taskIndex < planVertex.getParallelism(); taskIndex++) {
        vertexTasks.add(new ExecutionTask(taskId, taskIndex, workers.get(taskId)));
        taskId++;
      }
      StreamProcessor streamProcessor = ProcessBuilder
          .buildProcessor(planVertex.getStreamOperator());
      executionNode.setExecutionTaskList(vertexTasks);
      executionNode.setStreamProcessor(streamProcessor);
      idToExecutionNode.put(executionNode.getNodeId(), executionNode);
    }

    for (PlanEdge planEdge : planEdges) {
      int srcNodeId = planEdge.getSrcVertexId();
      int targetNodeId = planEdge.getTargetVertexId();

      ExecutionEdge executionEdge = new ExecutionEdge(srcNodeId, targetNodeId,
          planEdge.getPartition());
      idToExecutionNode.get(srcNodeId).addExecutionEdge(executionEdge);
    }

    List<ExecutionNode> executionNodes = idToExecutionNode.values().stream()
        .collect(Collectors.toList());
    return new ExecutionGraph(executionNodes);
  }
}

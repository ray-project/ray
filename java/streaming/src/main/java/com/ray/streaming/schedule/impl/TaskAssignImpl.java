package com.ray.streaming.schedule.impl;

import com.ray.streaming.core.graph.ExecutionEdge;
import com.ray.streaming.core.graph.ExecutionGraph;
import com.ray.streaming.core.graph.ExecutionNode;
import com.ray.streaming.core.graph.ExecutionTask;
import com.ray.streaming.core.processor.ProcessBuilder;
import com.ray.streaming.core.processor.StreamProcessor;
import com.ray.streaming.core.runtime.StreamWorker;
import com.ray.streaming.plan.Plan;
import com.ray.streaming.plan.PlanEdge;
import com.ray.streaming.plan.PlanVertex;
import com.ray.streaming.schedule.ITaskAssign;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.RayActor;

public class TaskAssignImpl implements ITaskAssign {

  /**
   * assign optimize logical plan to execution graph.
   * @param plan logical plan
   * @param workers ray actor
   * @return ExecutionGraph is Physical Execution Graph
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

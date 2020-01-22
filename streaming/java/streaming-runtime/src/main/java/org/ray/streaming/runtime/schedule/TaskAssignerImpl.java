package org.ray.streaming.runtime.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.ray.api.RayActor;
import org.ray.streaming.jobgraph.JobEdge;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobVertex;
import org.ray.streaming.runtime.core.graph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.graph.ExecutionTask;
import org.ray.streaming.runtime.worker.JobWorker;

public class TaskAssignerImpl implements TaskAssigner {

  /**
   * Assign an optimized logical plan to execution graph.
   *
   * @param jobGraph    The logical plan.
   * @param workers The worker actors.
   * @return The physical execution graph.
   */
  @Override
  public ExecutionGraph assign(JobGraph jobGraph, List<RayActor<JobWorker>> workers) {
    List<JobVertex> jobVertices = jobGraph.getJobVertexList();
    List<JobEdge> jobEdges = jobGraph.getJobEdgeList();

    int taskId = 0;
    Map<Integer, ExecutionNode> idToExecutionNode = new HashMap<>();
    for (JobVertex jobVertex : jobVertices) {
      ExecutionNode executionNode = new ExecutionNode(jobVertex.getVertexId(),
          jobVertex.getParallelism());
      executionNode.setNodeType(jobVertex.getVertexType());
      List<ExecutionTask> vertexTasks = new ArrayList<>();
      for (int taskIndex = 0; taskIndex < jobVertex.getParallelism(); taskIndex++) {
        vertexTasks.add(new ExecutionTask(taskId, taskIndex, workers.get(taskId)));
        taskId++;
      }
      executionNode.setExecutionTasks(vertexTasks);
      executionNode.setStreamOperator(jobVertex.getStreamOperator());
      idToExecutionNode.put(executionNode.getNodeId(), executionNode);
    }

    for (JobEdge jobEdge : jobEdges) {
      int srcNodeId = jobEdge.getSrcVertexId();
      int targetNodeId = jobEdge.getTargetVertexId();

      ExecutionEdge executionEdge = new ExecutionEdge(srcNodeId, targetNodeId,
          jobEdge.getPartition());
      idToExecutionNode.get(srcNodeId).addExecutionEdge(executionEdge);
      idToExecutionNode.get(targetNodeId).addInputEdge(executionEdge);
    }

    List<ExecutionNode> executionNodes = idToExecutionNode.values().stream()
        .collect(Collectors.toList());
    return new ExecutionGraph(executionNodes);
  }
}

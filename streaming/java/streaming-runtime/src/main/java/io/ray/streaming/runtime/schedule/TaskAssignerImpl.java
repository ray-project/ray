package io.ray.streaming.runtime.schedule;

import io.ray.api.BaseActor;
import io.ray.api.Ray;
import io.ray.api.function.PyActorClass;
import io.ray.streaming.jobgraph.JobEdge;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.jobgraph.JobVertex;
import io.ray.streaming.runtime.core.graph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.ExecutionNode;
import io.ray.streaming.runtime.core.graph.ExecutionTask;
import io.ray.streaming.runtime.worker.JobWorker;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskAssignerImpl implements TaskAssigner {

  /**
   * Assign an optimized logical plan to execution graph.
   *
   * @param jobGraph The logical plan.
   * @return The physical execution graph.
   */
  @Override
  public ExecutionGraph assign(JobGraph jobGraph) {
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
        vertexTasks.add(new ExecutionTask(taskId, taskIndex, createWorker(jobVertex)));
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
      idToExecutionNode.get(srcNodeId).addOutputEdge(executionEdge);
      idToExecutionNode.get(targetNodeId).addInputEdge(executionEdge);
    }

    List<ExecutionNode> executionNodes = new ArrayList<>(idToExecutionNode.values());
    return new ExecutionGraph(executionNodes);
  }

  private BaseActor createWorker(JobVertex jobVertex) {
    switch (jobVertex.getLanguage()) {
      case PYTHON:
        return Ray.createActor(
            new PyActorClass("ray.streaming.runtime.worker", "JobWorker"));
      case JAVA:
        return Ray.createActor(JobWorker::new);
      default:
        throw new UnsupportedOperationException(
            "Unsupported language " + jobVertex.getLanguage());

    }
  }
}

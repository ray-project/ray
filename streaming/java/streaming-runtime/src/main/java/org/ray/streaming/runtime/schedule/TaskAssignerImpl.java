package org.ray.streaming.runtime.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.streaming.plan.Plan;
import org.ray.streaming.plan.PlanEdge;
import org.ray.streaming.plan.PlanVertex;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.runtime.core.graph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.graph.ExecutionTask;
import org.ray.streaming.runtime.worker.JobWorker;

public class TaskAssignerImpl implements TaskAssigner {

  /**
   * Assign an optimized logical plan to execution graph.
   *
   * @param plan    The logical plan.
   * @return The physical execution graph.
   */
  @Override
  public ExecutionGraph assign(Plan plan) {
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
        vertexTasks.add(new ExecutionTask(taskId, taskIndex, createWorker(planVertex)));
        taskId++;
      }
      executionNode.setExecutionTasks(vertexTasks);
      executionNode.setStreamOperator(planVertex.getStreamOperator());
      idToExecutionNode.put(executionNode.getNodeId(), executionNode);
    }

    for (PlanEdge planEdge : planEdges) {
      int srcNodeId = planEdge.getSrcVertexId();
      int targetNodeId = planEdge.getTargetVertexId();

      ExecutionEdge executionEdge = new ExecutionEdge(srcNodeId, targetNodeId,
          planEdge.getPartition());
      idToExecutionNode.get(srcNodeId).addExecutionEdge(executionEdge);
      idToExecutionNode.get(targetNodeId).addInputEdge(executionEdge);
    }

    List<ExecutionNode> executionNodes = new ArrayList<>(idToExecutionNode.values());
    return new ExecutionGraph(executionNodes);
  }

  private RayActor createWorker(PlanVertex planVertex) {
    if (planVertex.getStreamOperator() instanceof PythonOperator) {
      return Ray.createPyActor("ray.streaming.runtime.worker", "JobWorker");
    } else {
      return Ray.createActor(JobWorker::new);
    }
  }
}

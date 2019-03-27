package org.ray.streaming.schedule.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.streaming.api.partition.impl.BroadcastPartition;
import org.ray.streaming.cluster.ResourceManager;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.graph.ExecutionNode;
import org.ray.streaming.core.graph.ExecutionNode.NodeType;
import org.ray.streaming.core.graph.ExecutionTask;
import org.ray.streaming.core.runtime.StreamWorker;
import org.ray.streaming.core.runtime.context.WorkerContext;
import org.ray.streaming.operator.impl.MasterOperator;
import org.ray.streaming.plan.Plan;
import org.ray.streaming.plan.PlanEdge;
import org.ray.streaming.plan.PlanVertex;
import org.ray.streaming.plan.VertexType;
import org.ray.streaming.schedule.IJobSchedule;
import org.ray.streaming.schedule.ITaskAssign;


public class JobScheduleImpl implements IJobSchedule {

  private Plan plan;
  private Map<String, Object> jobConfig;
  private ResourceManager resourceManager;
  private ITaskAssign taskAssign;

  public JobScheduleImpl(Map<String, Object> jobConfig) {
    this.resourceManager = new ResourceManager();
    this.taskAssign = new TaskAssignImpl();
    this.jobConfig = jobConfig;
  }

  /**
   * Schedule physical plan to execution graph, and call streaming worker to init and run.
   */
  @Override
  public void schedule(Plan plan) {
    this.plan = plan;
    addJobMaster(plan);
    List<RayActor<StreamWorker>> workers = this.resourceManager.createWorker(getPlanWorker());
    ExecutionGraph executionGraph = this.taskAssign.assign(this.plan, workers);

    List<ExecutionNode> executionNodes = executionGraph.getExecutionNodeList();
    List<RayObject<Boolean>> waits = new ArrayList<>();
    ExecutionTask masterTask = null;
    for (ExecutionNode executionNode : executionNodes) {
      List<ExecutionTask> executionTasks = executionNode.getExecutionTaskList();
      for (ExecutionTask executionTask : executionTasks) {
        if (executionNode.getNodeType() != NodeType.MASTER) {
          Integer taskId = executionTask.getTaskId();
          RayActor<StreamWorker> streamWorker = executionTask.getWorker();
          waits.add(Ray.call(StreamWorker::init, streamWorker,
              new WorkerContext(taskId, executionGraph, jobConfig)));
        } else {
          masterTask = executionTask;
        }
      }
    }
    Ray.wait(waits);

    Integer masterId = masterTask.getTaskId();
    RayActor<StreamWorker> masterWorker = masterTask.getWorker();
    Ray.call(StreamWorker::init, masterWorker,
        new WorkerContext(masterId, executionGraph, jobConfig)).get();
  }

  private void addJobMaster(Plan plan) {
    int masterVertexId = 0;
    int masterParallelism = 1;
    PlanVertex masterVertex = new PlanVertex(masterVertexId, masterParallelism, VertexType.MASTER,
        new MasterOperator());
    plan.getPlanVertexList().add(masterVertex);
    List<PlanVertex> planVertices = plan.getPlanVertexList();
    for (PlanVertex planVertex : planVertices) {
      if (planVertex.getVertexType() == VertexType.SOURCE) {
        PlanEdge planEdge = new PlanEdge(masterVertexId, planVertex.getVertexId(),
            new BroadcastPartition());
        plan.getPlanEdgeList().add(planEdge);
      }
    }
  }

  private int getPlanWorker() {
    List<PlanVertex> planVertexList = plan.getPlanVertexList();
    return planVertexList.stream().map(vertex -> vertex.getParallelism()).reduce(0, Integer::sum);
  }
}

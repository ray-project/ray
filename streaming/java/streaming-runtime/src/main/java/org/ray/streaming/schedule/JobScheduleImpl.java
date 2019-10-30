package org.ray.streaming.schedule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.streaming.cluster.ResourceManager;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.graph.ExecutionNode;
import org.ray.streaming.core.graph.ExecutionTask;
import org.ray.streaming.runtime.JobMaster;
import org.ray.streaming.runtime.JobWorker;
import org.ray.streaming.runtime.context.WorkerContext;
import org.ray.streaming.plan.Plan;
import org.ray.streaming.plan.PlanVertex;

public class JobScheduleImpl implements IJobSchedule {
  private Plan plan;
  private Map<String, Object> jobConfig;
  private ResourceManager resourceManager;
  private ITaskAssign taskAssign;

  public JobScheduleImpl() {
    this.resourceManager = new ResourceManager();
    this.taskAssign = new TaskAssignImpl();
  }

  /**
   * Schedule physical plan to execution graph, and call streaming worker to init and run.
   */
  @Override
  public void schedule(Plan plan, Map<String, Object> jobConfig) {
    this.jobConfig = jobConfig;
    this.plan = plan;
    Ray.init();

    List<RayActor<JobWorker>> workers = this.resourceManager.createWorker(getPlanWorker());
    ExecutionGraph executionGraph = this.taskAssign.assign(this.plan, workers);

    List<ExecutionNode> executionNodes = executionGraph.getExecutionNodeList();
    List<RayObject<Boolean>> waits = new ArrayList<>();
    for (ExecutionNode executionNode : executionNodes) {
      List<ExecutionTask> executionTasks = executionNode.getExecutionTasks();
      for (ExecutionTask executionTask : executionTasks) {
        int taskId = executionTask.getTaskId();
        RayActor<JobWorker> streamWorker = executionTask.getWorker();
        waits.add(Ray.call(JobWorker::init, streamWorker,
            new WorkerContext(taskId, executionGraph, jobConfig)));
      }
    }
    Ray.wait(waits);

    RayActor<JobMaster> jobMaster = Ray.createActor(JobMaster::new);
    Ray.call(JobMaster::init, jobMaster, executionGraph, jobConfig).get();
  }

  private int getPlanWorker() {
    List<PlanVertex> planVertexList = plan.getPlanVertexList();
    return planVertexList.stream().map(PlanVertex::getParallelism).reduce(0, Integer::sum);
  }
}

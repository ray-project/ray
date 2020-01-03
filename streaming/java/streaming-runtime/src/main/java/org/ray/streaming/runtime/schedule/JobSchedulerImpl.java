package org.ray.streaming.runtime.schedule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.streaming.plan.Plan;
import org.ray.streaming.plan.PlanVertex;
import org.ray.streaming.runtime.cluster.ResourceManager;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.graph.ExecutionTask;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.WorkerContext;
import org.ray.streaming.schedule.JobScheduler;

/**
 * JobSchedulerImpl schedules workers by the Plan and the resource information
 * from ResourceManager.
 */
public class JobSchedulerImpl implements JobScheduler {
  private Plan plan;
  private Map<String, Object> jobConfig;
  private ResourceManager resourceManager;
  private TaskAssigner taskAssigner;

  public JobSchedulerImpl() {
    this.resourceManager = new ResourceManager();
    this.taskAssigner = new TaskAssignerImpl();
  }

  /**
   * Schedule physical plan to execution graph, and call streaming worker to init and run.
   */
  @Override
  public void schedule(Plan plan, Map<String, Object> jobConfig) {
    this.jobConfig = jobConfig;
    this.plan = plan;
    System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");
    Ray.init();

    List<RayActor<JobWorker>> workers = this.resourceManager.createWorkers(getPlanWorker());
    ExecutionGraph executionGraph = this.taskAssigner.assign(this.plan, workers);

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
  }

  private int getPlanWorker() {
    List<PlanVertex> planVertexList = plan.getPlanVertexList();
    return planVertexList.stream().map(PlanVertex::getParallelism).reduce(0, Integer::sum);
  }
}

package org.ray.streaming.runtime.schedule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.streaming.plan.Plan;
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
  private Map<String, String> jobConfig;
  private TaskAssigner taskAssigner;

  public JobSchedulerImpl() {
    this.taskAssigner = new TaskAssignerImpl();
  }

  /**
   * Schedule physical plan to execution graph, and call streaming worker to init and run.
   */
  @Override
  public void schedule(Plan plan, Map<String, String> jobConfig) {
    this.jobConfig = jobConfig;
    this.plan = plan;
    if (Ray.internal() == null) {
      System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");
      Ray.init();
    }

    ExecutionGraph executionGraph = this.taskAssigner.assign(this.plan);

    List<ExecutionNode> executionNodes = executionGraph.getExecutionNodeList();
    List<RayObject<Boolean>> waits = new ArrayList<>();
    for (ExecutionNode executionNode : executionNodes) {
      List<ExecutionTask> executionTasks = executionNode.getExecutionTasks();
      for (ExecutionTask executionTask : executionTasks) {
        int taskId = executionTask.getTaskId();
        RayActor streamWorker = executionTask.getWorker();
        waits.add(Ray.call(JobWorker::init, streamWorker,
            new WorkerContext(taskId, executionGraph, jobConfig)));
      }
    }
    Ray.wait(waits);
  }

}

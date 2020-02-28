package org.ray.streaming.runtime.schedule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.streaming.api.Language;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.graph.ExecutionTask;
import org.ray.streaming.runtime.generated.RemoteCall;
import org.ray.streaming.runtime.python.GraphPbBuilder;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.WorkerContext;
import org.ray.streaming.schedule.JobScheduler;

/**
 * JobSchedulerImpl schedules workers by the Plan and the resource information
 * from ResourceManager.
 */
public class JobSchedulerImpl implements JobScheduler {
  private JobGraph jobGraph;
  private Map<String, String> jobConfig;
  private TaskAssigner taskAssigner;

  public JobSchedulerImpl() {
    this.taskAssigner = new TaskAssignerImpl();
  }

  /**
   * Schedule physical plan to execution graph, and call streaming worker to init and run.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void schedule(JobGraph jobGraph, Map<String, String> jobConfig) {
    this.jobConfig = jobConfig;
    this.jobGraph = jobGraph;
    if (Ray.internal() == null) {
      System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");
      Ray.init();
    }

    ExecutionGraph executionGraph = this.taskAssigner.assign(this.jobGraph);
    List<ExecutionNode> executionNodes = executionGraph.getExecutionNodeList();
    boolean hasPythonNode = executionNodes.stream()
        .allMatch(node -> node.getLanguage() == Language.PYTHON);
    RemoteCall.ExecutionGraph executionGraphPb = null;
    if (hasPythonNode) {
      executionGraphPb = new GraphPbBuilder().buildExecutionGraphPb(executionGraph);
    }
    List<RayObject<Object>> waits = new ArrayList<>();
    for (ExecutionNode executionNode : executionNodes) {
      List<ExecutionTask> executionTasks = executionNode.getExecutionTasks();
      for (ExecutionTask executionTask : executionTasks) {
        int taskId = executionTask.getTaskId();
        RayActor worker = executionTask.getWorker();
        switch (executionNode.getLanguage()) {
          case JAVA:
            RayActor<JobWorker> jobWorker = (RayActor<JobWorker>) worker;
            waits.add(Ray.call(JobWorker::init, jobWorker,
                new WorkerContext(taskId, executionGraph, jobConfig)));
            break;
          case PYTHON:
            byte[] workerContextBytes = buildPythonWorkerContext(
                taskId, executionGraphPb, jobConfig);
            waits.add(Ray.callPy((RayPyActor) worker,
                "init", workerContextBytes));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported language " + executionNode.getLanguage());
        }
      }
    }
    Ray.wait(waits);
  }

  private byte[] buildPythonWorkerContext(
      int taskId,
      RemoteCall.ExecutionGraph executionGraphPb,
      Map<String, String> jobConfig) {
    return RemoteCall.WorkerContext.newBuilder()
        .setTaskId(taskId)
        .putAllConf(jobConfig)
        .setGraph(executionGraphPb)
        .build()
        .toByteArray();
  }

}

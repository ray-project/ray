package io.ray.streaming.runtime.schedule;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;
import io.ray.streaming.api.Language;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.core.graph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.ExecutionNode;
import io.ray.streaming.runtime.core.graph.ExecutionTask;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.python.GraphPbBuilder;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.WorkerContext;
import io.ray.streaming.schedule.JobScheduler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    ExecutionGraph executionGraph = this.taskAssigner.assign(this.jobGraph);
    List<ExecutionNode> executionNodes = executionGraph.getExecutionNodeList();
    boolean hasPythonNode = executionNodes.stream()
        .anyMatch(node -> node.getLanguage() == Language.PYTHON);
    RemoteCall.ExecutionGraph executionGraphPb = null;
    if (hasPythonNode) {
      executionGraphPb = new GraphPbBuilder().buildExecutionGraphPb(executionGraph);
    }
    List<ObjectRef<Object>> waits = new ArrayList<>();
    for (ExecutionNode executionNode : executionNodes) {
      List<ExecutionTask> executionTasks = executionNode.getExecutionTasks();
      for (ExecutionTask executionTask : executionTasks) {
        int taskId = executionTask.getTaskId();
        BaseActorHandle worker = executionTask.getWorker();
        switch (executionNode.getLanguage()) {
          case JAVA:
            ActorHandle<JobWorker> jobWorker = (ActorHandle<JobWorker>) worker;
            waits.add(jobWorker.call(JobWorker::init,
                new WorkerContext(taskId, executionGraph, jobConfig)));
            break;
          case PYTHON:
            byte[] workerContextBytes = buildPythonWorkerContext(
                taskId, executionGraphPb, jobConfig);
            waits.add(((PyActorHandle)worker).call(new PyActorMethod("init", Object.class),
                workerContextBytes));
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

package io.ray.streaming.runtime.worker.context;

import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import io.ray.api.ActorHandle;
import io.ray.api.id.ActorId;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.streaming.runtime.config.global.CommonConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.python.GraphPbBuilder;
import java.io.Serializable;
import java.util.Map;

/** Job worker context of java type. */
public class JobWorkerContext implements Serializable {

  /** JobMaster actor. */
  private ActorHandle<JobMaster> master;

  /** Worker's vertex info. */
  private ExecutionVertex executionVertex;

  public JobWorkerContext(ActorHandle<JobMaster> master, ExecutionVertex executionVertex) {
    this.master = master;
    this.executionVertex = executionVertex;
  }

  public ActorId getWorkerActorId() {
    return executionVertex.getWorkerActorId();
  }

  public int getWorkerId() {
    return executionVertex.getExecutionVertexId();
  }

  public String getWorkerName() {
    return executionVertex.getExecutionVertexName();
  }

  public Map<String, String> getConfig() {
    return executionVertex.getWorkerConfig();
  }

  public ActorHandle<JobMaster> getMaster() {
    return master;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  public Map<String, String> getConf() {
    return getExecutionVertex().getWorkerConfig();
  }

  public String getJobName() {
    return getConf().get(CommonConfig.JOB_NAME);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("workerId", getWorkerId())
        .add("workerName", getWorkerName())
        .add("config", getConfig())
        .toString();
  }

  public byte[] getPythonWorkerContextBytes() {
    // create python worker context
    RemoteCall.ExecutionVertexContext executionVertexContext =
        new GraphPbBuilder().buildExecutionVertexContext(executionVertex);

    byte[] contextBytes =
        RemoteCall.PythonJobWorkerContext.newBuilder()
            .setMasterActor(ByteString.copyFrom((((NativeActorHandle) (master)).toBytes())))
            .setExecutionVertexContext(executionVertexContext)
            .build()
            .toByteArray();

    return contextBytes;
  }
}

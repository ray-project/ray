package io.ray.streaming.runtime.worker.context;

import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import io.ray.api.RayActor;
import io.ray.api.id.ActorId;

import io.ray.runtime.actor.NativeRayActor;

import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.python.GraphPbBuilder;
import io.ray.streaming.runtime.serialization.JavaSerializer;
import java.io.Serializable;

/**
 * Job worker context of java type.
 */
public class JobWorkerContext implements Serializable {

  /**
   * JobMaster actor.
   */
  private RayActor<JobMaster> master;

  /**
   * Worker's vertex info.
   */
  private ExecutionVertex executionVertex;

  public JobWorkerContext(
      RayActor<JobMaster> master,
      ExecutionVertex executionVertex) {
    this.master = master;
    this.executionVertex = executionVertex;
  }

  public int getWorkerId() {
    return executionVertex.getId();
  }

  public ActorId getActorId() {
    return executionVertex.getWorkerActorId();
  }

  public String getWorkerName() {
    return executionVertex.getVertexName();
  }

  public RayActor<JobMaster> getMaster() {
    return master;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("workerId", getWorkerId())
      .add("workerName", getWorkerName())
      .toString();
  }

  public byte[] getPythonWorkerContextBytes() {
    // create python worker context
    RemoteCall.SubExecutionGraph subGraphPb =
      new GraphPbBuilder().buildSubExecutionGraph(executionVertex);

    byte[] contextBytes = RemoteCall.WorkerContext.newBuilder()
      .setActorId(executionVertex.getWorkerActor().getId().toString())
      .setMasterActor(ByteString.copyFrom((((NativeRayActor) (master)).toBytes())))
      .setSubGraph(subGraphPb)
      .build()
      .toByteArray();

    return contextBytes;
  }

}

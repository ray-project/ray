package io.ray.streaming.runtime.worker.context;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.state.backend.AbstractKeyStateBackend;
import io.ray.streaming.state.backend.KeyStateBackend;
import io.ray.streaming.state.backend.OperatorStateBackend;
import io.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import io.ray.streaming.state.keystate.desc.MapStateDescriptor;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.ListState;
import io.ray.streaming.state.keystate.state.MapState;
import io.ray.streaming.state.keystate.state.ValueState;
import java.util.Map;

/**
 * Use Ray to implement RuntimeContext.
 */
public class StreamingRuntimeContext implements RuntimeContext {
  /**
   * Backend for keyed state. This might be empty if we're not on a keyed stream.
   */
  protected transient KeyStateBackend keyStateBackend;
  /**
   * Backend for operator state. This might be empty
   */
  protected transient OperatorStateBackend operatorStateBackend;
  private int taskId;
  private int taskIndex;
  private int parallelism;
  private Long checkpointId;
  private Map<String, String> config;

  public StreamingRuntimeContext(ExecutionVertex executionVertex, Map<String, String> config,
      int parallelism) {
    this.taskId = executionVertex.getExecutionVertexId();
    this.config = config;
    this.taskIndex = executionVertex.getExecutionVertexIndex();
    this.parallelism = parallelism;
  }

  @Override
  public int getTaskId() {
    return taskId;
  }

  @Override
  public int getTaskIndex() {
    return taskIndex;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public Map<String, String> getConfig() {
    return config;
  }

  @Override
  public Map<String, String> getJobConfig() {
    return config;
  }

  @Override
  public Long getCheckpointId() {
    return checkpointId;
  }

  @Override
  public void setCheckpointId(long checkpointId) {
    if (this.keyStateBackend != null) {
      this.keyStateBackend.setCheckpointId(checkpointId);
    }
    if (this.operatorStateBackend != null) {
      this.operatorStateBackend.setCheckpointId(checkpointId);
    }
    this.checkpointId = checkpointId;
  }

  @Override
  public void setCurrentKey(Object key) {
    this.keyStateBackend.setCurrentKey(key);
  }

  @Override
  public KeyStateBackend getKeyStateBackend() {
    return keyStateBackend;
  }

  @Override
  public void setKeyStateBackend(KeyStateBackend keyStateBackend) {
    this.keyStateBackend = keyStateBackend;
  }

  @Override
  public <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor) {
    stateSanityCheck(stateDescriptor, this.keyStateBackend);
    return this.keyStateBackend.getValueState(stateDescriptor);
  }

  @Override
  public <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor) {
    stateSanityCheck(stateDescriptor, this.keyStateBackend);
    return this.keyStateBackend.getListState(stateDescriptor);
  }

  @Override
  public <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor) {
    stateSanityCheck(stateDescriptor, this.keyStateBackend);
    return this.keyStateBackend.getMapState(stateDescriptor);
  }

  protected void stateSanityCheck(AbstractStateDescriptor stateDescriptor,
                                  AbstractKeyStateBackend backend) {
    Preconditions.checkNotNull(stateDescriptor, "The state properties must not be null");
    Preconditions.checkNotNull(backend, "backend must not be null");
  }
}

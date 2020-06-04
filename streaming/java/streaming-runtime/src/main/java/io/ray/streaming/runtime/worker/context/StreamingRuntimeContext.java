package io.ray.streaming.runtime.worker.context;

import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.state.backend.KeyStateBackend;
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

  private int taskId;
  private int subTaskIndex;
  private int parallelism;
  private Map<String, String> config;

  public StreamingRuntimeContext(
      ExecutionVertex executionVertex,
      Map<String, String> config,
      int parallelism) {
    this.taskId = executionVertex.getId();
    this.subTaskIndex = executionVertex.getVertexIndex();
    this.parallelism = parallelism;
    this.config = config;
  }

  @Override
  public int getTaskId() {
    return taskId;
  }

  @Override
  public int getTaskIndex() {
    return subTaskIndex;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public Long getCheckpointId() {
    return null;
  }

  @Override
  public void setCheckpointId(long checkpointId) {
  }

  @Override
  public Map<String, String> getConfig() {
    return null;
  }

  @Override
  public Map<String, String> getJobConfig() {
    return config;
  }

  @Override
  public void setCurrentKey(Object key) {

  }

  @Override
  public KeyStateBackend getKeyStateBackend() {
    return null;
  }

  @Override
  public void setKeyStateBackend(KeyStateBackend keyStateBackend) {

  }

  @Override
  public <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor) {
    return null;
  }

  @Override
  public <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor) {
    return null;
  }

  @Override
  public <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor) {
    return null;
  }

}

package io.ray.streaming.api.context;

import io.ray.streaming.state.backend.KeyStateBackend;
import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import io.ray.streaming.state.keystate.desc.MapStateDescriptor;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.ListState;
import io.ray.streaming.state.keystate.state.MapState;
import io.ray.streaming.state.keystate.state.ValueState;
import java.util.Map;

/** Encapsulate the runtime information of a streaming task. */
public interface RuntimeContext {

  int getTaskId();

  int getTaskIndex();

  int getParallelism();

  /** Returns config of current function */
  Map<String, String> getConfig();

  /** Returns config of the job */
  Map<String, String> getJobConfig();

  Long getCheckpointId();

  void setCheckpointId(long checkpointId);

  void setCurrentKey(Object key);

  KeyStateBackend getKeyStateBackend();

  void setKeyStateBackend(KeyStateBackend keyStateBackend);

  <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor);

  <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor);

  <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor);
}

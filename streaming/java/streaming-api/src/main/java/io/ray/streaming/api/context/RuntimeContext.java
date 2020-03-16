package io.ray.streaming.api.context;

import java.util.Map;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.ValueState;

/**
 * Encapsulate the runtime information of a streaming task.
 */
public interface RuntimeContext {

  int getTaskId();

  int getTaskIndex();

  int getParallelism();

  Long getCheckpointId();

  void setCheckpointId(long checkpointId);

  Long getMaxBatch();

  Map<String, String> getConfig();

  void setCurrentKey(Object key);

  KeyStateBackend getKeyStateBackend();

  void setKeyStateBackend(KeyStateBackend keyStateBackend);

  <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor);

  <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor);

  <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor);
}

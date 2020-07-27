package io.ray.streaming.runtime.state;

import io.ray.streaming.runtime.config.StreamingGlobalConfig;
import io.ray.streaming.runtime.config.types.StateBackendType;
import io.ray.streaming.runtime.state.impl.MemoryStateBackend;

public class StateBackendFactory {

  public static StateBackend getStateBackend(final StreamingGlobalConfig config) {
    StateBackend stateBackend;
    StateBackendType type = StateBackendType.valueOf(
        config.stateBackendConfig.stateBackendType().toUpperCase());

    switch (type) {
      case MEMORY:
        stateBackend = new MemoryStateBackend(config.stateBackendConfig);
        break;
      default:
        throw new RuntimeException("Unsupported state backend type.");
    }
    return stateBackend;
  }
}
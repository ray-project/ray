package io.ray.streaming.runtime.state;

import io.ray.streaming.runtime.config.StreamingGlobalConfig;
import io.ray.streaming.runtime.config.types.ContextBackendType;
import io.ray.streaming.runtime.state.impl.AtomicFsBackend;
import io.ray.streaming.runtime.state.impl.MemoryContextBackend;

public class StateBackendFactory {

  public static ContextBackend getStateBackend(final StreamingGlobalConfig config) {
    ContextBackend contextBackend;
    ContextBackendType type = ContextBackendType.valueOf(
        config.stateBackendConfig.stateBackendType().toUpperCase());

    switch (type) {
      case MEMORY:
        contextBackend = new MemoryContextBackend(config.stateBackendConfig);
        break;
      case LOCAL_FILE:
        contextBackend = new AtomicFsBackend(config.stateBackendConfig);
        break;
      default:
        throw new RuntimeException("Unsupported state backend type.");
    }
    return contextBackend;
  }
}
package io.ray.streaming.runtime.context;

import io.ray.streaming.runtime.config.StreamingGlobalConfig;
import io.ray.streaming.runtime.config.types.ContextBackendType;
import io.ray.streaming.runtime.context.impl.AtomicFsBackend;
import io.ray.streaming.runtime.context.impl.MemoryContextBackend;

public class ContextBackendFactory {

  public static ContextBackend getContextBackend(final StreamingGlobalConfig config) {
    ContextBackend contextBackend;
    ContextBackendType type =
        ContextBackendType.valueOf(config.contextBackendConfig.stateBackendType().toUpperCase());

    switch (type) {
      case MEMORY:
        contextBackend = new MemoryContextBackend(config.contextBackendConfig);
        break;
      case LOCAL_FILE:
        contextBackend = new AtomicFsBackend(config.contextBackendConfig);
        break;
      default:
        throw new RuntimeException("Unsupported context backend type.");
    }
    return contextBackend;
  }
}

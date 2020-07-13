package io.ray.streaming.runtime.config.global;

import org.aeonbits.owner.Config;

public interface StateBackendConfig extends Config {

  String STATE_BACKEND_TYPE = "streaming.state-backend.type";

  @Config.DefaultValue(value = "memory")
  @Key(value = STATE_BACKEND_TYPE)
  String stateBackendType();
}

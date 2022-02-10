package io.ray.api.runtimeenv;

import io.ray.api.Ray;
import java.util.HashMap;
import java.util.Map;

public class RuntimeEnvBuilder {
  private Map<String, String> envVars = new HashMap<>();

  public RuntimeEnvBuilder addEnvVar(String key, String value) {
    envVars.put(key, value);
    return this;
  }

  public RuntimeEnv build() {
    return Ray.internal().createRuntimeEnv(envVars);
  }
}

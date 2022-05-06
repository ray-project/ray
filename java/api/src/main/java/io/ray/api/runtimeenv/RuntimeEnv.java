package io.ray.api.runtimeenv;

import io.ray.api.Ray;
import java.util.HashMap;
import java.util.Map;

/** This is an experimental API to let you set runtime environments for your actors. */
public interface RuntimeEnv {

  String toJsonBytes();

  public static class Builder {

    private Map<String, String> envVars = new HashMap<>();

    public Builder addEnvVar(String key, String value) {
      envVars.put(key, value);
      return this;
    }

    public RuntimeEnv build() {
      return Ray.internal().createRuntimeEnv(envVars);
    }
  }
}

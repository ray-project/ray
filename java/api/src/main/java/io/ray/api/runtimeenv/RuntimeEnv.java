package io.ray.api.runtimeenv;

import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This is an experimental API to let you set runtime environments for your actors. */
public interface RuntimeEnv {

  String toJsonBytes();

  public static class Builder {

    private Map<String, String> envVars = new HashMap<>();
    private List<String> jars = new ArrayList<>();

    /** Add environment variable as runtime environment for the actor or job. */
    public Builder addEnvVar(String key, String value) {
      envVars.put(key, value);
      return this;
    }

    /**
     * Add the jars as runtime environment for the actor or job. We now support both `.jar` files
     * and `.zip` files.
     */
    public Builder addJars(List<String> jars) {
      this.jars.addAll(jars);
      return this;
    }

    public RuntimeEnv build() {
      return Ray.internal().createRuntimeEnv(envVars, jars);
    }
  }
}

package io.ray.api.runtimeenv;

import io.ray.api.Ray;

/** This is an experimental API to let you set runtime environments info for your actors. */
public interface RuntimeEnvInfo {

  String toJsonBytes();

  public static class Builder {

    String serializedRuntimeEnv;

    public Builder setSerializedRuntimeEnv(String serializedRuntimeEnv) {
      this.serializedRuntimeEnv = serializedRuntimeEnv;
      return this;
    }

    public RuntimeEnvInfo build() {
      return Ray.internal().createRuntimeEnvInfo(serializedRuntimeEnv);
    }
  }
}

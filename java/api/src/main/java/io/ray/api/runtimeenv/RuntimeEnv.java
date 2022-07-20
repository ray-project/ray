package io.ray.api.runtimeenv;

import io.ray.api.Ray;
import io.ray.api.exception.RuntimeEnvException;

/** This is an experimental API to let you set runtime environments for your actors. */
public interface RuntimeEnv {

  void set(String name, Object value);

  public void setJsonStr(String name, String jsonStr) throws RuntimeEnvException;

  public <T> T get(String name, Class<T> classOfT) throws RuntimeEnvException;

  public String getJsonStr(String name) throws RuntimeEnvException;

  public void remove(String name);

  public String serialize() throws RuntimeEnvException;

  public static RuntimeEnv deserialize(String serializedRuntimeEnv) {
    return Ray.internal().deserializeRuntimeEnv(serializedRuntimeEnv);
  }

  public static class Builder {
    public RuntimeEnv build() {
      return Ray.internal().createRuntimeEnv();
    }
  }
}

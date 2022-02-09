package io.ray.runtime.runtimeenv;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.ray.api.runtimeenv.RuntimeEnv;

import java.util.HashMap;
import java.util.Map;

public class RuntimeEnvImpl implements RuntimeEnv {

  private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

  private Map<String, String> envVars = new HashMap<>();

  public RuntimeEnvImpl(Map<String, String> envVars) {
    /// TODO: Do we need clone instead of ref?
    this.envVars = envVars;
  }

  @Override
  public String toJsonBytes() {
    Map<String, Object> json = new HashMap<>();
    if (!envVars.isEmpty()) {
      /// DO NOT hardcode this key.
      json.put("env_vars", envVars);
    }
    return GSON.toJson(json);
  }

}

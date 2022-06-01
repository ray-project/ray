package io.ray;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RuntimeEnv {
  JsonObject runtimeEnvs = new JsonObject();
  static private JsonParser jsonParser = new JsonParser();
  private Gson gson = new Gson();

  public void set(String name, Object typedRuntimeEnv) {
    runtimeEnvs.add(name, gson.toJsonTree(typedRuntimeEnv));
  }

  public <T> T get(String name, Class<T> classOfT) {
    return gson.fromJson(runtimeEnvs.get(name), classOfT);
  }

  public void remove(String name) {
    runtimeEnvs.remove(name);
  }

  public String serialize() {
    return gson.toJson(runtimeEnvs);
  }

  public static RuntimeEnv deserialize(String serializedRuntimeEnv) {
    RuntimeEnv runtimeEnv = new RuntimeEnv();
    runtimeEnv.runtimeEnvs = jsonParser.parse(serializedRuntimeEnv).getAsJsonObject();
    return runtimeEnv;
  }
}

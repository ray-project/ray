package io.ray.serve.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtil {
  private static Gson gson = getGson();

  public GsonUtil() {}

  private static Gson getGson() {
    return new GsonBuilder().disableHtmlEscaping().create();
  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    return gson.fromJson(json, clazz);
  }

  public static String toJson(Object obj) {
    return gson.toJson(obj);
  }
}

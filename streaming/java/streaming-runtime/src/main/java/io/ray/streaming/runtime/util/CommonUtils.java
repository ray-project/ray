package io.ray.streaming.runtime.util;

import java.util.Map;

/** Common tools. */
public class CommonUtils {

  public static Map<String, Object> strMapToObjectMap(Map<String, String> srcMap) {
    Map<String, Object> destMap = (Map) srcMap;
    return destMap;
  }
}

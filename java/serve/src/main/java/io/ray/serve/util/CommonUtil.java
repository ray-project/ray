package io.ray.serve.util;

import org.apache.commons.lang3.StringUtils;

public class CommonUtil {

  public static String formatActorName(String controllerName, String actorName) {
    if (StringUtils.isBlank(controllerName)) {
      return actorName;
    }
    return controllerName + ":" + actorName;
  }
}

package io.ray.serve.util;

import org.apache.commons.lang3.StringUtils;

public class CommonUtil {

  public static String getDeploymentName(String deploymentDef) {
    return StringUtils.substringAfterLast(StringUtils.substringAfterLast(deploymentDef, "."), "$");
  }
}

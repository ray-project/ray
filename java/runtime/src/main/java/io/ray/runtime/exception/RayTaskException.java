package io.ray.runtime.exception;

import io.ray.runtime.util.NetworkUtil;
import io.ray.runtime.util.SystemUtil;

public class RayTaskException extends RayException {

  public RayTaskException(String message) {
    super(message);
  }

  public RayTaskException(String message, Throwable cause) {
    super(
        String.format(
            "(pid=%d, ip=%s) %s", SystemUtil.pid(), NetworkUtil.getIpAddress(null), message),
        cause);
  }
}

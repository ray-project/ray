package io.ray.api.exception;

public class RayTaskException extends RayException {

  public RayTaskException(String message) {
    super(message);
  }

  public RayTaskException(int pid, String ipAddress, String message, Throwable cause) {
    super(String.format("(pid=%d, ip=%s) %s", pid, ipAddress, message), cause);
  }
}

package org.ray.hook.runtime;

/**
 * method mode switch at runtime
 */
public class MethodSwitcher {

  public static final ThreadLocal<Boolean> IsRemoteCall = new ThreadLocal<>();

  public static final ThreadLocal<byte[]> MethodId = new ThreadLocal<>();

  public static boolean execute(byte[] id) {
    Boolean hooking = IsRemoteCall.get();
    if (Boolean.TRUE.equals(hooking)) {
      MethodId.set(id);
      return true;
    } else {
      return false;
    }
  }
}

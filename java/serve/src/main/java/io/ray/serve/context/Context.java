package io.ray.serve.context;

public class Context {
  private static final ThreadLocal<RequestContext> contextInfo = new ThreadLocal<>();

  public static void setContextInfo(RequestContext info) {
      contextInfo.set(info);
  }

  public static RequestContext getContextInfo() {
      return contextInfo.get();
  }

  public static void clear() {
      contextInfo.remove();
  }
}

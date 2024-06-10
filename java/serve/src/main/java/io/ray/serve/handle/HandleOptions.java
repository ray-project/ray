package io.ray.serve.handle;

import java.io.Serializable;

/** Options for each ServeHandle instances. These fields are immutable. */
public class HandleOptions implements Serializable {

  private static final long serialVersionUID = -279077795726949172L;
  private String methodName = "call";

  public String getMethodName() {
    return methodName;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }
}

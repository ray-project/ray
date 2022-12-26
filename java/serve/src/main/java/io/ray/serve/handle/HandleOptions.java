package io.ray.serve.handle;

/** Options for each ServeHandle instances. These fields are immutable. */
public class HandleOptions {

  private String methodName = "call";

  private String signature = "";

  public String getMethodName() {
    return methodName;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  public String getSignature() {
    return signature;
  }

  public void setSignature(String signature) {
    this.signature = signature;
  }
}

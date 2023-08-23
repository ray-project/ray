package io.ray.serve.handle;

/** Options for each ServeHandle instances. These fields are immutable. */
public class HandleOptions {

  private String methodName = "call";

  private String multiplexedModelId = "";

  private Boolean isStreaming = false;

  public String getMethodName() {
    return methodName;
  }

  public void setMethodName(String methodName) {
    this.methodName = methodName;
  }

  public String getMultiplexedModelId() {
    return multiplexedModelId;
  }

  public void setMultiplexedModelId(String multiplexedModelId) {
    this.multiplexedModelId = multiplexedModelId;
  }

  public Boolean getStreaming() {
    return isStreaming;
  }

  public void setStreaming(Boolean streaming) {
    isStreaming = streaming;
  }
}

package io.ray.serve;

import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import java.util.Map;

public class DummyServeController implements ServeController {

  private Map<String, EndpointInfo> endpoints;

  @Override
  public byte[] getAllEndpoints() {
    EndpointSet.Builder builder = EndpointSet.newBuilder();
    builder.putAllEndpoints(endpoints);
    return builder.build().toByteArray();
  }

  public void setEndpoints(Map<String, EndpointInfo> endpoints) {
    this.endpoints = endpoints;
  }
}

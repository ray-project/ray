package io.ray.serve;

import io.ray.serve.controller.ServeController;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.poll.LongPollRequest;
import io.ray.serve.util.ServeProtoUtil;
import java.util.Map;

public class DummyServeController implements ServeController {
  private Map<String, EndpointInfo> endpoints;

  private byte[] longPollResult;

  private String rootUrl;

  public DummyServeController(String rootUrl) {
    this.rootUrl = rootUrl;
  }

  @Override
  public byte[] getAllEndpoints() {
    EndpointSet.Builder builder = EndpointSet.newBuilder();
    builder.putAllEndpoints(endpoints);
    return builder.build().toByteArray();
  }

  public boolean setEndpoints(byte[] endpoints) {
    this.endpoints = ServeProtoUtil.parseEndpointSet(endpoints);
    return true;
  }

  public boolean setLongPollResult(byte[] longPollResult) {
    this.longPollResult = longPollResult;
    return true;
  }

  @Override
  public byte[] listenForChange(LongPollRequest longPollRequest) {
    return longPollResult;
  }

  @Override
  public String getRootUrl() {
    return rootUrl;
  }

  public void setRootUrl(String rootUrl) {
    this.rootUrl = rootUrl;
  }
}

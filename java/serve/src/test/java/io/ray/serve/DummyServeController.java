package io.ray.serve;

import io.ray.serve.controller.ServeController;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.poll.LongPollRequest;
import io.ray.serve.poll.LongPollResult;
import java.util.Map;

public class DummyServeController implements ServeController {

  private Map<String, EndpointInfo> endpoints;

  private LongPollResult longPollResult;

  private String rootUrl;

  private String checkpointPath;

  public DummyServeController(String rootUrl, String checkpointPath) {
    this.rootUrl = rootUrl;
    this.checkpointPath = checkpointPath;
  }

  @Override
  public byte[] getAllEndpoints() {
    EndpointSet.Builder builder = EndpointSet.newBuilder();
    builder.putAllEndpoints(endpoints);
    return builder.build().toByteArray();
  }

  public void setEndpoints(Map<String, EndpointInfo> endpoints) {
    this.endpoints = endpoints;
  }

  public boolean setLongPollResult(LongPollResult longPollResult) {
    this.longPollResult = longPollResult;
    return true;
  }

  @Override
  public LongPollResult listenForChange(LongPollRequest longPollRequest) {
    return longPollResult;
  }

  @Override
  public String getRootUrl() {
    return rootUrl;
  }

  public void setRootUrl(String rootUrl) {
    this.rootUrl = rootUrl;
  }

  @Override
  public String getCheckpointPath() {
    return checkpointPath;
  }
}

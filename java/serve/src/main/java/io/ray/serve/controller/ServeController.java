package io.ray.serve.controller;

import io.ray.serve.poll.LongPollRequest;

public interface ServeController {
  byte[] getAllEndpoints();

  byte[] listenForChange(LongPollRequest longPollRequest);

  String getRootUrl();
}

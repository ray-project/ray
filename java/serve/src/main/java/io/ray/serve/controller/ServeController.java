package io.ray.serve.controller;

import io.ray.serve.poll.LongPollRequest;
import io.ray.serve.poll.LongPollResult;

public interface ServeController {

  byte[] getAllEndpoints();

  LongPollResult listenForChange(LongPollRequest longPollRequest);
}

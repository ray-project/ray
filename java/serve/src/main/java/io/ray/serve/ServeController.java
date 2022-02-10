package io.ray.serve;

import io.ray.serve.poll.LongPollRequest;
import io.ray.serve.poll.LongPollResult;

public interface ServeController {

  byte[] getAllEndpoints();

  LongPollResult listenForChange(LongPollRequest longPollRequest);
}

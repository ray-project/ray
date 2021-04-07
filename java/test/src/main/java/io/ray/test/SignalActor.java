package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.util.concurrent.Semaphore;

public class SignalActor {

  private Semaphore semaphore;

  public SignalActor() {
    this.semaphore = new Semaphore(0);
  }

  public int sendSignal() {
    this.semaphore.release();
    return 0;
  }

  public int waitSignal() throws InterruptedException {
    this.semaphore.acquire();
    return 0;
  }

  public static ActorHandle<SignalActor> create() {
    return Ray.actor(SignalActor::new).setMaxConcurrency(2).remote();
  }
}

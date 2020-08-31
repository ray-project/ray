package io.ray.docdemo;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.util.concurrent.TimeUnit;

public class UsingActorsDemo2 {

  public static class Counter {

    private int counter = 0;

    public void inc() {
      counter += 1;
    }

    public int getCounter() {
      return counter;
    }
  }

  public static class MyRayApp {

    public static void foo(ActorHandle<Counter> counter) throws InterruptedException {
      for (int i = 0; i < 1000; i++) {
        TimeUnit.MILLISECONDS.sleep(100);
        counter.task(Counter::inc).remote();
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {
    Ray.init();

    ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();

    // Start some tasks that use the actor.
    for (int i = 0; i < 3; i++) {
      Ray.task(MyRayApp::foo, counter).remote();
    }

    // Print the counter value.
    for (int i = 0; i < 10; i++) {
      TimeUnit.SECONDS.sleep(1);
      System.out.println(counter.task(Counter::getCounter).remote().get());
    }
  }
}

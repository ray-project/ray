package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;

public class Counter {

  private int value;

  private ActorHandle<Counter> childActor;

  public Counter(int initValue) {
    this.value = initValue;
  }

  public int getValue() {
    return value;
  }

  public void increase(int delta) {
    value += delta;
  }

  public int increaseAndGet(int delta) {
    value += delta;
    return value;
  }

  public String echo(String str) {
    return str;
  }

  public String createChildActor(String actorName) {
    childActor = Ray.actor(Counter::new, 0).setName(actorName).remote();
    Assert.assertEquals(Integer.valueOf(0), childActor.task(Counter::getValue).remote().get());
    return "OK";
  }

  public static class NestedActor {

    public NestedActor(String s) {
      str = s;
    }

    public String concat(String s) {
      return str + s;
    }

    private String str;
  }
}

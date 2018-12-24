package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.id.UniqueId;

public class StressTest extends BaseTest {

  public static int echo(int x) {
    return x;
  }

  @Test
  public void testSubmittingTasks() {
    for (int numIterations : ImmutableList.of(1, 10, 100, 1000)) {
      int numTasks = 1000 / numIterations;
      for (int i = 0; i < numIterations; i++) {
        List<UniqueId> resultIds = new ArrayList<>();
        for (int j = 0; j < numTasks; j++) {
          resultIds.add(Ray.call(StressTest::echo, 1).getId());
        }
        for (Integer result : Ray.<Integer>get(resultIds)) {
          Assert.assertEquals(result, Integer.valueOf(1));
        }
      }
    }
  }

  @Test
  public void testDependency() {
    RayObject<Integer> x = Ray.call(StressTest::echo, 1);
    for (int i = 0; i < 1000; i++) {
      x = Ray.call(StressTest::echo, x);
    }
    Assert.assertEquals(x.get(), Integer.valueOf(1));
  }

  public static class Actor {

    public int ping() {
      return 1;
    }
  }

  public static class Worker {

    private RayActor<Actor> actor;

    public Worker(RayActor<Actor> actor) {
      this.actor = actor;
    }

    public int ping(int n) {
      List<UniqueId> objectIds = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        objectIds.add(Ray.call(Actor::ping, actor).getId());
      }
      int sum = 0;
      for (Integer result : Ray.<Integer>get(objectIds)) {
        sum += result;
      }
      return sum;
    }
  }

  @Test
  public void testSubmittingManyTasksToOneActor() {
    RayActor<Actor> actor = Ray.createActor(Actor::new);
    List<UniqueId> objectIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      RayActor<Worker> worker = Ray.createActor(Worker::new, actor);
      objectIds.add(Ray.call(Worker::ping, worker, 100).getId());
    }
    for (Integer result : Ray.<Integer>get(objectIds)) {
      Assert.assertEquals(result, Integer.valueOf(100));
    }
  }

  @Test
  public void testPuttingAndGettingManyObjects() {
    Integer objectToPut = 1;
    List<RayObject<Integer>> objects = new ArrayList<>();
    for (int i = 0; i < 100_000; i++) {
      objects.add(Ray.put(objectToPut));
    }
    for (RayObject<Integer> object : objects) {
      Assert.assertEquals(object.get(), objectToPut);
    }
  }
}

package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class StressTest extends BaseTest {

  public static int echo(int x) {
    return x;
  }

  public void testSubmittingTasks() {
    for (int numIterations : ImmutableList.of(1, 10, 100, 1000)) {
      int numTasks = 1000 / numIterations;
      for (int i = 0; i < numIterations; i++) {
        List<ObjectRef<Integer>> results = new ArrayList<>();
        for (int j = 0; j < numTasks; j++) {
          results.add(Ray.task(StressTest::echo, 1).remote());
        }

        for (Integer result : Ray.get(results)) {
          Assert.assertEquals(result, Integer.valueOf(1));
        }
      }
    }
  }

  public void testDependency() {
    ObjectRef<Integer> x = Ray.task(StressTest::echo, 1).remote();
    for (int i = 0; i < 1000; i++) {
      x = Ray.task(StressTest::echo, x).remote();
    }

    Assert.assertEquals(x.get(), Integer.valueOf(1));
  }

  public static class Actor {

    public int ping() {
      return 1;
    }
  }

  public static class Worker {

    private ActorHandle<Actor> actor;

    public Worker(ActorHandle<Actor> actor) {
      this.actor = actor;
    }

    public int ping(int n) {
      List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        objectRefs.add(actor.task(Actor::ping).remote());
      }
      int sum = 0;
      for (Integer result : Ray.get(objectRefs)) {
        sum += result;
      }
      return sum;
    }
  }

  public void testSubmittingManyTasksToOneActor() throws Exception {
    ActorHandle<Actor> actor = Ray.actor(Actor::new).remote();
    List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ActorHandle<Worker> worker = Ray.actor(Worker::new, actor).remote();
      objectRefs.add(worker.task(Worker::ping, 100).remote());
    }

    for (Integer result : Ray.get(objectRefs)) {
      Assert.assertEquals(result, Integer.valueOf(100));
    }
  }

  public void testPuttingAndGettingManyObjects() {
    Integer objectToPut = 1;
    List<ObjectRef<Integer>> objects = new ArrayList<>();
    for (int i = 0; i < 100_000; i++) {
      objects.add(Ray.put(objectToPut));
    }

    for (ObjectRef<Integer> object : objects) {
      Assert.assertEquals(object.get(), objectToPut);
    }
  }
}

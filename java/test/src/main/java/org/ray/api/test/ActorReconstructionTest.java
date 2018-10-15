package org.ray.api.test;

import static org.ray.runtime.util.SystemUtil.pid;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;

@RunWith(MyRunner.class)
public class ActorReconstructionTest {

  @RayRemote()
  public static class Counter {

    private int value = 0;

    public int increase(int delta) {
      value += delta;
      return value;
    }

    public int getPid() {
      return pid();
    }
  }

  @Test
  public void testActorReconstruction() throws InterruptedException, IOException {
    ActorCreationOptions options = new ActorCreationOptions(new HashMap<>(), 1);
    RayActor<Counter> actor = Ray.createActor(Counter::new, options);
    // Call increase 3 times.
    for (int i = 0; i < 3; i++) {
      Ray.call(Counter::increase, actor, 1).get();
    }

    // Kill the actor process.
    int pid = Ray.call(Counter::getPid, actor).get();
    Runtime.getRuntime().exec("kill -9 " + pid);
    // Wait for the actor to be killed.
    TimeUnit.SECONDS.sleep(1);

    // Try calling increase on this actor again and check the value is now 4.
    int value = Ray.call(Counter::increase, actor, 1).get();
    Assert.assertEquals(value, 4);

    // Kill the actor process again.
    pid = Ray.call(Counter::getPid, actor).get();
    Runtime.getRuntime().exec("kill -9 " + pid);
    TimeUnit.SECONDS.sleep(1);

    // Try calling increase on this actor again and this should fail.
    try {
      Ray.call(Counter::increase, actor, 1).get();
      Assert.fail("The above task didn't fail.");
    } catch (StringIndexOutOfBoundsException e) {
      // Raylet backend will put invalid data in task's result to indicate the task has failed.
      // Thus, Java deserialization will fail and throw `StringIndexOutOfBoundsException`.
      // TODO(hchen): we should use object's metadata to indicate task failure,
      // instead of throwing this exception.
    }
  }
}

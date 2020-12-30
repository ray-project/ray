package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.util.SystemUtil;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ActorRestartTest extends BaseTest {

  public static class Counter {

    protected int value = 0;

    private boolean wasCurrentActorRestarted = false;

    public Counter() {
      wasCurrentActorRestarted = Ray.getRuntimeContext().wasCurrentActorRestarted();
    }

    public boolean wasCurrentActorRestarted() {
      return wasCurrentActorRestarted;
    }

    public int increase() {
      value += 1;
      return value;
    }

    public boolean askRayWasCurrentActorRestarted() {
      return Ray.getRuntimeContext().wasCurrentActorRestarted();
    }

    public int getPid() {
      return SystemUtil.pid();
    }
  }

  public void testActorRestart() throws InterruptedException, IOException {
    ActorHandle<Counter> actor = Ray.actor(Counter::new).setMaxRestarts(1).remote();
    // Call increase 3 times.
    for (int i = 0; i < 3; i++) {
      actor.task(Counter::increase).remote().get();
    }

    Assert.assertFalse(actor.task(Counter::wasCurrentActorRestarted).remote().get());

    // Kill the actor process.
    killActorProcess(actor);

    int value = actor.task(Counter::increase).remote().get();
    Assert.assertEquals(value, 1);

    Assert.assertTrue(actor.task(Counter::wasCurrentActorRestarted).remote().get());

    // Kill the actor process again.
    killActorProcess(actor);

    // Try calling increase on this actor again and this should fail.
    try {
      actor.task(Counter::increase).remote().get();
      Assert.fail("The above task didn't fail.");
    } catch (RayActorException e) {
      // We should receive a RayActorException because the actor is dead.
    }
  }

  @Test
  public void testWasCurrentActorRestartedInActorTask() throws IOException, InterruptedException {
    ActorHandle<Counter> actor = Ray.actor(Counter::new).setMaxRestarts(1).remote();
    actor.task(Counter::getPid).remote().get();
    Assert.assertFalse(actor.task(Counter::askRayWasCurrentActorRestarted).remote().get());
    // Kill the actor process.
    killActorProcess(actor);
    int value = actor.task(Counter::increase).remote().get();
    Assert.assertEquals(value, 1);
    Assert.assertTrue(actor.task(Counter::wasCurrentActorRestarted).remote().get());
    // Kill the actor process again.
    killActorProcess(actor);
    // Try calling increase on this actor again and this should fail.
    try {
      actor.task(Counter::increase).remote().get();
      Assert.fail("The above task didn't fail.");
    } catch (RayActorException e) {
      // We should receive a RayActorException because the actor is dead.
    }
  }

  /**
   * The helper to kill a counter actor.
   * @param actor The counter actor to be killed.
   * @return The pid of the actor.
   */
  private static void killActorProcess(ActorHandle<Counter> actor)
      throws IOException, InterruptedException {
    // Kill the actor process.
    int pid = actor.task(Counter::getPid).remote().get();
    Process p = Runtime.getRuntime().exec("kill -9 " + pid);
    // Wait for the actor to be killed.
    TimeUnit.SECONDS.sleep(1);
  }
}

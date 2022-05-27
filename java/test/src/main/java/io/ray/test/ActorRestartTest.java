package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayException;
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

    public boolean checkWasCurrentActorRestartedInActorCreationTask() {
      return wasCurrentActorRestarted;
    }

    public int increase() {
      value += 1;
      return value;
    }

    public boolean checkWasCurrentActorRestartedInActorTask() {
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

    // Check if actor was restarted.
    Assert.assertFalse(
        actor.task(Counter::checkWasCurrentActorRestartedInActorCreationTask).remote().get());
    Assert.assertFalse(
        actor.task(Counter::checkWasCurrentActorRestartedInActorTask).remote().get());

    // Kill the actor process.
    killActorProcess(actor);

    waitForActorAlive(actor);
    int value = actor.task(Counter::increase).remote().get();
    Assert.assertEquals(value, 1);

    // Check if actor was restarted again.
    Assert.assertTrue(
        actor.task(Counter::checkWasCurrentActorRestartedInActorCreationTask).remote().get());
    Assert.assertTrue(actor.task(Counter::checkWasCurrentActorRestartedInActorTask).remote().get());

    // Kill the actor process again.
    killActorProcess(actor);

    // Try calling increase on this actor again and this should fail.
    Assert.assertThrows(
        RayActorException.class, () -> actor.task(Counter::increase).remote().get());
  }

  /** The helper to kill a counter actor. */
  private static void killActorProcess(ActorHandle<Counter> actor)
      throws IOException, InterruptedException {
    // Kill the actor process.
    int pid = actor.task(Counter::getPid).remote().get();
    Process p = Runtime.getRuntime().exec("kill -9 " + pid);
    // Wait for the actor to be killed.
    TimeUnit.SECONDS.sleep(1);
  }

  private static void waitForActorAlive(ActorHandle<Counter> actor) {
    Assert.assertTrue(
        TestUtils.waitForCondition(
            () -> {
              try {
                actor.task(Counter::getPid).remote().get();
                return true;
              } catch (RayException e) {
                return false;
              }
            },
            10000));
  }
}

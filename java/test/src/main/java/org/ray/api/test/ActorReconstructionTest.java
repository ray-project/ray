package org.ray.api.test;

import static org.ray.runtime.util.SystemUtil.pid;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.ray.api.Checkpointable;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.exception.RayActorException;
import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ActorReconstructionTest extends BaseTest {

  @RayRemote()
  public static class Counter {

    protected int value = 0;

    private boolean wasCurrentActorReconstructed = false;

    public Counter() {
      wasCurrentActorReconstructed = Ray.getRuntimeContext().wasCurrentActorReconstructed();
    }

    public boolean wasCurrentActorReconstructed() {
      return wasCurrentActorReconstructed;
    }

    public int increase() {
      value += 1;
      return value;
    }

    public int getPid() {
      return pid();
    }
  }

  @Test
  public void testActorReconstruction() throws InterruptedException, IOException {
    TestUtils.skipTestUnderSingleProcess();
    ActorCreationOptions options =
        new ActorCreationOptions.Builder().setMaxReconstructions(1).createActorCreationOptions();
    RayActor<Counter> actor = Ray.createActor(Counter::new, options);
    // Call increase 3 times.
    for (int i = 0; i < 3; i++) {
      Ray.call(Counter::increase, actor).get();
    }

    Assert.assertFalse(Ray.call(Counter::wasCurrentActorReconstructed, actor).get());

    // Kill the actor process.
    int pid = Ray.call(Counter::getPid, actor).get();
    Runtime.getRuntime().exec("kill -9 " + pid);
    // Wait for the actor to be killed.
    TimeUnit.SECONDS.sleep(1);

    // Try calling increase on this actor again and check the value is now 4.
    int value = Ray.call(Counter::increase, actor).get();
    Assert.assertEquals(value, 4);

    Assert.assertTrue(Ray.call(Counter::wasCurrentActorReconstructed, actor).get());

    // Kill the actor process again.
    pid = Ray.call(Counter::getPid, actor).get();
    Runtime.getRuntime().exec("kill -9 " + pid);
    TimeUnit.SECONDS.sleep(1);

    // Try calling increase on this actor again and this should fail.
    try {
      Ray.call(Counter::increase, actor).get();
      Assert.fail("The above task didn't fail.");
    } catch (RayActorException e) {
      // We should receive a RayActorException because the actor is dead.
    }
  }

  public static class CheckpointableCounter extends Counter implements Checkpointable {

    private boolean resumedFromCheckpoint = false;
    private boolean increaseCalled = false;

    @Override
    public int increase() {
      increaseCalled = true;
      return super.increase();
    }

    public boolean wasResumedFromCheckpoint() {
      return resumedFromCheckpoint;
    }

    @Override
    public boolean shouldCheckpoint(CheckpointContext checkpointContext) {
      // Checkpoint the actor when value is increased to 3.
      boolean shouldCheckpoint = increaseCalled && value == 3;
      increaseCalled = false;
      return shouldCheckpoint;
    }

    @Override
    public void saveCheckpoint(ActorId actorId, UniqueId checkpointId) {
      // In practice, user should save the checkpoint id and data to a persistent store.
      // But for simplicity, we don't do that in this unit test.
    }

    @Override
    public UniqueId loadCheckpoint(ActorId actorId, List<Checkpoint> availableCheckpoints) {
      // Restore previous value and return checkpoint id.
      this.value = 3;
      this.resumedFromCheckpoint = true;
      return availableCheckpoints.get(availableCheckpoints.size() - 1).checkpointId;
    }

    @Override
    public void checkpointExpired(ActorId actorId, UniqueId checkpointId) {
    }
  }

  @Test
  public void testActorCheckpointing() throws IOException, InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    ActorCreationOptions options =
        new ActorCreationOptions.Builder().setMaxReconstructions(1).createActorCreationOptions();
    RayActor<CheckpointableCounter> actor = Ray.createActor(CheckpointableCounter::new, options);
    // Call increase 3 times.
    for (int i = 0; i < 3; i++) {
      Ray.call(CheckpointableCounter::increase, actor).get();
    }
    // Assert that the actor wasn't resumed from a checkpoint.
    Assert.assertFalse(Ray.call(CheckpointableCounter::wasResumedFromCheckpoint, actor).get());
    int pid = Ray.call(CheckpointableCounter::getPid, actor).get();
    Runtime.getRuntime().exec("kill -9 " + pid);
    // Wait for the actor to be killed.
    TimeUnit.SECONDS.sleep(1);

    // Try calling increase on this actor again and check the value is now 4.
    int value = Ray.call(CheckpointableCounter::increase, actor).get();
    Assert.assertEquals(value, 4);
    // Assert that the actor was resumed from a checkpoint.
    Assert.assertTrue(Ray.call(CheckpointableCounter::wasResumedFromCheckpoint, actor).get());
  }
}


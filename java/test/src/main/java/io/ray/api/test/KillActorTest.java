package io.ray.api.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.Ray;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import io.ray.api.TestUtils;
import io.ray.api.exception.RayActorException;
import io.ray.api.options.ActorCreationOptions;
import java.util.function.BiConsumer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
public class KillActorTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.raylet.config.num_workers_per_process_java");
  }

  public static class HangActor {

    public String ping() {
      return "pong";
    }

    public boolean hang() throws InterruptedException {
      while (true) {
        Thread.sleep(1000);
      }
    }
  }

  public static class KillerActor {

    public void kill(RayActor<?> actor, boolean noReconstruction) {
      actor.kill(noReconstruction);
    }
  }

  private static void localKill(RayActor<?> actor, boolean noReconstruction) {
    actor.kill(noReconstruction);
  }

  private static void remoteKill(RayActor<?> actor, boolean noReconstruction) {
    RayActor<KillerActor> killer = Ray.createActor(KillerActor::new);
    killer.call(KillerActor::kill, actor, noReconstruction);
  }

  private void testKillActor(BiConsumer<RayActor<?>, Boolean> kill, boolean noReconstruction) {
    TestUtils.skipTestUnderSingleProcess();

    ActorCreationOptions options =
        new ActorCreationOptions.Builder().setMaxReconstructions(1).createActorCreationOptions();
    RayActor<HangActor> actor = Ray.createActor(HangActor::new, options);
    RayObject<Boolean> result = actor.call(HangActor::hang);
    // The actor will hang in this task.
    Assert.assertEquals(0, Ray.wait(ImmutableList.of(result), 1, 500).getReady().size());

    // Kill the actor
    kill.accept(actor, noReconstruction);
    // The get operation will fail with RayActorException
    Assert.expectThrows(RayActorException.class, result::get);

    try {
      // Sleep 1s here to make sure the driver has received the actor notification
      // (of state RECONSTRUCTING or DEAD).
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (noReconstruction) {
      // The actor should not be reconstructed.
      Assert.expectThrows(RayActorException.class, () -> actor.call(HangActor::hang).get());
    } else {
      Assert.assertEquals(actor.call(HangActor::ping).get(), "pong");
    }
  }

  public void testLocalKill() {
    testKillActor(KillActorTest::localKill, false);
    testKillActor(KillActorTest::localKill, true);
  }

  public void testRemoteKill() {
    testKillActor(KillActorTest::remoteKill, false);
    testKillActor(KillActorTest::remoteKill, true);
  }
}

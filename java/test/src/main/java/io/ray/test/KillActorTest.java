package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
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

    public void kill(ActorHandle<?> actor, boolean noRestart) {
      actor.kill(noRestart);
    }
  }

  private static void localKill(ActorHandle<?> actor, boolean noRestart) {
    actor.kill(noRestart);
  }

  private static void remoteKill(ActorHandle<?> actor, boolean noRestart) {
    ActorHandle<KillerActor> killer = Ray.createActor(KillerActor::new);
    killer.call(KillerActor::kill, actor, noRestart);
  }

  private void testKillActor(BiConsumer<ActorHandle<?>, Boolean> kill, boolean noRestart) {
    TestUtils.skipTestUnderSingleProcess();

    ActorCreationOptions options =
        new ActorCreationOptions.Builder().setMaxRestarts(1).createActorCreationOptions();
    ActorHandle<HangActor> actor = Ray.createActor(HangActor::new, options);
    ObjectRef<Boolean> result = actor.call(HangActor::hang);
    // The actor will hang in this task.
    Assert.assertEquals(0, Ray.wait(ImmutableList.of(result), 1, 500).getReady().size());

    // Kill the actor
    kill.accept(actor, noRestart);
    // The get operation will fail with RayActorException
    Assert.expectThrows(RayActorException.class, result::get);

    try {
      // Sleep 1s here to make sure the driver has received the actor notification
      // (of state RESTARTING or DEAD).
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (noRestart) {
      // The actor should not be restarted.
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

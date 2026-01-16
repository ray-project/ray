package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import java.util.function.BiConsumer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class KillActorTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.raylet.startup-token", "0");
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

    public void killWithoutRestart(ActorHandle<?> actor) {
      actor.kill();
    }
  }

  private static void localKill(ActorHandle<?> actor, boolean noRestart) {
    actor.kill(noRestart);
  }

  private static void remoteKill(ActorHandle<?> actor, boolean noRestart) {
    ActorHandle<KillerActor> killer = Ray.actor(KillerActor::new).remote();
    killer.task(KillerActor::kill, actor, noRestart).remote();
  }

  private void testKillActor(BiConsumer<ActorHandle<?>, Boolean> kill, boolean noRestart) {
    ActorHandle<HangActor> actor = Ray.actor(HangActor::new).setMaxRestarts(1).remote();
    // Wait for the actor to be created.
    actor.task(HangActor::ping).remote().get();
    ObjectRef<Boolean> result = actor.task(HangActor::hang).remote();
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
      Assert.expectThrows(
          RayActorException.class, () -> actor.task(HangActor::hang).remote().get());
    } else {
      Assert.assertEquals(actor.task(HangActor::ping).remote().get(), "pong");
    }
  }

  public void testLocalKill() {
    testKillActor(KillActorTest::localKill, false);
    testKillActor(KillActorTest::localKill, true);
    testKillActor((actorHandle, noRestart) -> actorHandle.kill(), true);
  }

  public void testRemoteKill() {
    testKillActor(KillActorTest::remoteKill, false);
    testKillActor(KillActorTest::remoteKill, true);
    testKillActor(
        (actor, noRestart) -> {
          ActorHandle<KillerActor> killer = Ray.actor(KillerActor::new).remote();
          killer.task(KillerActor::killWithoutRestart, actor).remote();
        },
        true);
  }
}

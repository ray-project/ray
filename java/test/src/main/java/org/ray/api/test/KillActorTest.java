package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.function.Consumer;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.ray.api.exception.RayActorException;
import org.ray.api.options.ActorCreationOptions;
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

  @RayRemote
  public static class HangActor {

    public boolean hang() throws InterruptedException {
      while (true) {
        Thread.sleep(1000);
      }
    }
  }

  @RayRemote
  public static class KillerActor {

    public void kill(RayActor<?> actor) {
      Ray.killActor(actor);
    }
  }

  private static void localKill(RayActor<?> actor) {
    Ray.killActor(actor);
  }

  private static void remoteKill(RayActor<?> actor) {
    RayActor<KillerActor> killer = Ray.createActor(KillerActor::new);
    Ray.call(KillerActor::kill, killer, actor);
  }

  private void testKillActor(Consumer<RayActor<?>> kill) {
    TestUtils.skipTestUnderSingleProcess();
    TestUtils.skipTestIfDirectActorCallDisabled();

    ActorCreationOptions options =
        new ActorCreationOptions.Builder().setMaxReconstructions(1).createActorCreationOptions();
    RayActor<HangActor> actor = Ray.createActor(HangActor::new, options);
    RayObject<Boolean> result = Ray.call(HangActor::hang, actor);
    // The actor will hang in this task.
    Assert.assertEquals(0, Ray.wait(ImmutableList.of(result), 1, 500).getReady().size());

    // Kill the actor
    kill.accept(actor);
    // The get operation will fail with RayActorException
    Assert.expectThrows(RayActorException.class, result::get);

    // The actor should not be reconstructed.
    Assert.expectThrows(RayActorException.class, () -> Ray.call(HangActor::hang, actor).get());
  }

  public void testLocalKill() {
    testKillActor(KillActorTest::localKill);
  }

  public void testRemoteKill() {
    testKillActor(KillActorTest::remoteKill);
  }
}

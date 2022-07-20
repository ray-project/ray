package io.ray.test;

import static io.ray.runtime.util.SystemUtil.pid;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.util.SystemUtil;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ExitActorTest2 extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.raylet.startup-token", "0");
  }

  private static class ExitingActor {
    private final Thread thread;

    public ExitingActor() {
      thread =
          new Thread(
              () -> {
                try {
                  TimeUnit.MINUTES.sleep(1);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              });
      // Start a non-daemon thread
      thread.start();
    }

    public int getPid() {
      return pid();
    }

    public void exit() {
      Ray.exitActor();
    }
  }

  public void testExitActorWithUserCreatedThread() {
    ActorHandle<ExitingActor> actor = Ray.actor(ExitingActor::new).remote();
    int pid = actor.task(ExitingActor::getPid).remote().get();
    Assert.assertTrue(SystemUtil.isProcessAlive(pid));
    actor.task(ExitingActor::exit).remote();
    Assert.assertTrue(TestUtils.waitForCondition(() -> !SystemUtil.isProcessAlive(pid), 10000));
  }
}

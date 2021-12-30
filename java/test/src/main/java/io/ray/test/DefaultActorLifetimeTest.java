package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.options.ActorLifetime;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.util.SystemUtil;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class DefaultActorLifetimeTest {

  private static class OwnerActor {
    private ActorHandle<ChildActor> childActor;

    public ActorHandle<ChildActor> createChildActor(ActorLifetime childActorLifetime) {
      if (childActorLifetime == null) {
        childActor = Ray.actor(ChildActor::new).remote();
      } else {
        childActor = Ray.actor(ChildActor::new).setLifetime(childActorLifetime).remote();
      }
      if ("ok".equals(childActor.task(ChildActor::ready).remote().get())) {
        return childActor;
      }
      return null;
    }

    int getPid() {
      return SystemUtil.pid();
    }

    String ready() {
      return "ok";
    }
  }

  private static class ChildActor {
    String ready() {
      return "ok";
    }
  }

  public static boolean internalTestDefaultActorLifetime(
      ActorLifetime defaultActorLifetime, ActorLifetime childActorLifetime) {
    if (defaultActorLifetime != null) {
      System.setProperty("ray.job.default-actor-lifetime", defaultActorLifetime.name());
    }
    try {
      System.setProperty("ray.job.num-java-workers-per-process", "1");
      Ray.init();

      /// 1. create owner and invoke createChildActor.
      ActorHandle<OwnerActor> owner = Ray.actor(OwnerActor::new).remote();
      ActorHandle<ChildActor> child =
          owner.task(OwnerActor::createChildActor, childActorLifetime).remote().get();
      Assert.assertEquals("ok", child.task(ChildActor::ready).remote().get());
      int ownerPid = owner.task(OwnerActor::getPid).remote().get();

      /// 2. Kill owner and make sure it's dead.
      Runtime.getRuntime().exec("kill -9 " + ownerPid);
      TimeUnit.SECONDS.sleep(1);

      Supplier<Boolean> isOwnerDead =
          () -> {
            try {
              owner.task(OwnerActor::ready).remote().get();
              return false;
            } catch (RayActorException e) {
              return true;
            }
          };
      Assert.assertTrue(TestUtils.waitForCondition(isOwnerDead, 3000));

      /// 3. Assert child state.
      Supplier<Boolean> isChildDead =
          () -> {
            try {
              child.task(ChildActor::ready).remote().get();
              return false;
            } catch (RayActorException e) {
              return true;
            }
          };

      Supplier<Boolean> childNotDead =
          () -> {
            try {
              child.task(ChildActor::ready).remote().get();
              return true;
            } catch (RayActorException e) {
              return false;
            }
          };

      if (childActorLifetime != null) {
        /// childActorLifetime is specified at runtime.
        if (childActorLifetime == ActorLifetime.DETACHED) {
          Assert.assertTrue(TestUtils.waitForCondition(childNotDead, 5000));
        } else {
          Assert.assertTrue(TestUtils.waitForCondition(isChildDead, 5000));
        }
      } else {
        /// Code path of not specifying childActorLifetime, so it's
        /// depends on the default actor lifetime.
        if (defaultActorLifetime == null) {
          Assert.assertTrue(TestUtils.waitForCondition(isChildDead, 5000));
        } else if (defaultActorLifetime == ActorLifetime.DETACHED) {
          TimeUnit.SECONDS.sleep(5);
          Assert.assertTrue(childNotDead.get());
        } else {
          Assert.assertTrue(TestUtils.waitForCondition(isChildDead, 5000));
        }
      }
      return true;
    } catch (Throwable th) {
      return false;
    } finally {
      Ray.shutdown();
    }
  }

  @DataProvider
  public static Object[][] parameters() {
    return new Object[][] {
      {null, null},
      {null, ActorLifetime.DETACHED},
      {null, ActorLifetime.NON_DETACHED},
      {ActorLifetime.DETACHED, null},
      {ActorLifetime.DETACHED, ActorLifetime.DETACHED},
      {ActorLifetime.DETACHED, ActorLifetime.NON_DETACHED},
      {ActorLifetime.NON_DETACHED, null},
      {ActorLifetime.NON_DETACHED, ActorLifetime.DETACHED},
      {ActorLifetime.NON_DETACHED, ActorLifetime.NON_DETACHED},
    };
  }

  @Test(
      groups = {"cluster"},
      dataProvider = "parameters")
  public void testDefaultActorLifetime(ActorLifetime defaultActorLifetime, ActorLifetime childActor) {
    Assert.assertTrue(internalTestDefaultActorLifetime(defaultActorLifetime, childActor));
  }
}

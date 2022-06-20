package io.ray.test;

import static io.ray.runtime.util.SystemUtil.pid;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.options.ActorCreationOptions;
import io.ray.runtime.task.TaskExecutor;
import io.ray.runtime.util.SystemUtil;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ExitActorTest extends BaseTest {

  private static class ExitingActor {

    int counter = 0;

    public Integer incr() {
      return ++counter;
    }

    public int getPid() {
      return pid();
    }

    public int getSizeOfActorContextMap() {
      TaskExecutor taskExecutor = TestUtils.getRuntime().getTaskExecutor();
      try {
        Field field = TaskExecutor.class.getDeclaredField("actorContextMap");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(taskExecutor)).size();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public boolean exit() {
      Ray.exitActor();
      return false;
    }
  }

  public void testExitActor() throws IOException, InterruptedException {
    ActorHandle<ExitingActor> actor =
        Ray.actor(ExitingActor::new).setMaxRestarts(ActorCreationOptions.INFINITE_RESTART).remote();
    Assert.assertEquals(1, (int) (actor.task(ExitingActor::incr).remote().get()));
    int pid = actor.task(ExitingActor::getPid).remote().get();
    Runtime.getRuntime().exec("kill -9 " + pid);

    while (true) {
      TimeUnit.SECONDS.sleep(1);
      try {
        actor.task(ExitingActor::getPid).remote().get();
        break;
      } catch (RayActorException e) {
        continue;
      }
    }

    // Make sure this actor can be reconstructed.
    Assert.assertEquals(1, (int) actor.task(ExitingActor::incr).remote().get());

    // `exitActor` will exit the actor without reconstructing.
    ObjectRef<Boolean> obj = actor.task(ExitingActor::exit).remote();
    Assert.assertThrows(RayActorException.class, obj::get);
  }

  public void testExitActorWithDynamicOptions() {
    ActorHandle<ExitingActor> actor =
        Ray.actor(ExitingActor::new)
            .setMaxRestarts(ActorCreationOptions.INFINITE_RESTART)
            // Set dummy JVM options to start a worker process with only one worker.
            .setJvmOptions(ImmutableList.of("-Ddummy=value"))
            .remote();
    int pid = actor.task(ExitingActor::getPid).remote().get();
    Assert.assertTrue(SystemUtil.isProcessAlive(pid));
    ObjectRef<Boolean> obj1 = actor.task(ExitingActor::exit).remote();
    Assert.assertThrows(RayActorException.class, obj1::get);
    // Now the actor shouldn't be reconstructed anymore.
    Assert.assertThrows(
        RayActorException.class, () -> actor.task(ExitingActor::getPid).remote().get());
    // Now the worker process should be dead.
    Assert.assertTrue(TestUtils.waitForCondition(() -> !SystemUtil.isProcessAlive(pid), 5000));
  }
}

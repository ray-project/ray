package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.JobId;
import io.ray.api.runtimecontext.ActorInfo;
import io.ray.api.runtimecontext.ActorState;
import io.ray.api.runtimecontext.Address;
import io.ray.api.runtimecontext.NodeInfo;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class GetActorInfoTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.head-args.0", "--num-cpus=5");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.head-args.0");
  }

  private static class Echo {

    public String echo(String str) {
      return str;
    }
  }

  public void testGetAllActorInfo() {
    List<NodeInfo> nodes = Ray.getRuntimeContext().getAllNodeInfo();
    Assert.assertEquals(nodes.size(), 1);
    final NodeInfo thisNode = nodes.get(0);
    final Address thisAddress =
        new Address(thisNode.nodeId, thisNode.nodeAddress, thisNode.nodeManagerPort);

    final int numActors = 5;
    ArrayList<ActorHandle<Echo>> echos = new ArrayList<>(numActors);
    for (int i = 0; i < numActors; ++i) {
      ActorHandle<Echo> echo = Ray.actor(Echo::new).remote();
      echos.add(echo);
    }

    ArrayList<ObjectRef<String>> objs = new ArrayList<>();
    echos.forEach(echo -> objs.add(echo.task(Echo::echo, "hello").remote()));
    Ray.wait(objs);

    List<ActorInfo> actorInfo = Ray.getRuntimeContext().getAllActorInfo();
    Assert.assertEquals(actorInfo.size(), 5);
    actorInfo.forEach(
        info -> {
          Assert.assertEquals(info.name, "");
          Assert.assertTrue(echos.stream().anyMatch(echo -> echo.getId().equals(info.actorId)));
          Assert.assertEquals(info.numRestarts, 0);
          /// Because `node.nodeManagerPort` is not `actorInfo.address.port`, we should
          /// not just equal them.
          Assert.assertEquals(info.address.nodeId, thisAddress.nodeId);
          Assert.assertEquals(info.address.ip, thisAddress.ip);
        });
  }

  public void testGetActorsByJobIdAndState() {
    final int numActors = 5;
    ArrayList<ActorHandle<Echo>> echos = new ArrayList<>(numActors);
    for (int i = 0; i < numActors; ++i) {
      ActorHandle<Echo> echo = Ray.actor(Echo::new).setResource("CPU", 1.0).remote();
      echos.add(echo);
    }
    ArrayList<ObjectRef<String>> objs = new ArrayList<>();
    echos.forEach(echo -> objs.add(echo.task(Echo::echo, "hello").remote()));
    Ray.get(objs, 10000);

    // get all actors
    List<ActorInfo> actorInfo = Ray.getRuntimeContext().getAllActorInfo(null, null);
    Assert.assertEquals(actorInfo.size(), 5);

    // Filtered by job id
    JobId jobId = Ray.getRuntimeContext().getCurrentJobId();
    actorInfo = Ray.getRuntimeContext().getAllActorInfo(jobId, null);
    Assert.assertEquals(actorInfo.size(), 5);

    // Filtered by actor sate
    actorInfo = Ray.getRuntimeContext().getAllActorInfo(null, ActorState.ALIVE);
    Assert.assertEquals(actorInfo.size(), 5);

    actorInfo = Ray.getRuntimeContext().getAllActorInfo(null, ActorState.DEPENDENCIES_UNREADY);
    Assert.assertEquals(actorInfo.size(), 0);

    actorInfo = Ray.getRuntimeContext().getAllActorInfo(null, ActorState.RESTARTING);
    Assert.assertEquals(actorInfo.size(), 0);

    ActorHandle<Echo> actor = Ray.actor(Echo::new).setResource("CPU", 1.0).remote();
    Assert.assertTrue(
        TestUtils.waitForCondition(
            () ->
                Ray.getRuntimeContext().getAllActorInfo(null, ActorState.PENDING_CREATION).size()
                    == 1,
            5000));

    actor.kill(true);
    Assert.assertTrue(
        TestUtils.waitForCondition(
            () -> Ray.getRuntimeContext().getAllActorInfo(null, ActorState.DEAD).size() == 1,
            5000));
  }
}

package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.runtimecontext.ActorInfo;
import io.ray.api.runtimecontext.Address;
import io.ray.api.runtimecontext.NodeInfo;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class GetActorInfoTest extends BaseTest {

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
}

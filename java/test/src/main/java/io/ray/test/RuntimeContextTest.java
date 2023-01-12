package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.runtime.gcs.GcsClient;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class RuntimeContextTest extends BaseTest {

  private static JobId JOB_ID = getJobId();

  private static JobId getJobId() {
    // Must be stable across different processes.
    byte[] bytes = new byte[JobId.LENGTH];
    Arrays.fill(bytes, (byte) 127);
    return JobId.fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.job.id", JOB_ID.toString());
  }

  // TODO(MisterLin1995 ): Fix JobConfig related Java test.
  @Test(enabled = false)
  public void testRuntimeContextInDriver() {
    Assert.assertEquals(JOB_ID, Ray.getRuntimeContext().getCurrentJobId());
    Assert.assertNotEquals(Ray.getRuntimeContext().getCurrentTaskId(), TaskId.NIL);
  }

  public static class RuntimeContextTester {

    public String testRuntimeContext(ActorId actorId) {
      /// test getCurrentJobId
      Assert.assertEquals(JOB_ID, Ray.getRuntimeContext().getCurrentJobId());
      /// test getCurrentTaskId
      Assert.assertNotEquals(Ray.getRuntimeContext().getCurrentTaskId(), TaskId.NIL);
      /// test getCurrentActorId
      Assert.assertEquals(actorId, Ray.getRuntimeContext().getCurrentActorId());

      /// test getCurrentNodeId
      UniqueId currNodeId = Ray.getRuntimeContext().getCurrentNodeId();
      GcsClient gcsClient = TestUtils.getRuntime().getGcsClient();
      List<NodeInfo> allNodeInfo = gcsClient.getAllNodeInfo();
      Assert.assertEquals(allNodeInfo.size(), 1);
      Assert.assertEquals(allNodeInfo.get(0).nodeId, currNodeId);

      return "ok";
    }
  }

  // TODO(MisterLin1995 ): Fix JobConfig related Java test.
  @Test(enabled = false)
  public void testRuntimeContextInActor() {
    ActorHandle<RuntimeContextTester> actor = Ray.actor(RuntimeContextTester::new).remote();
    Assert.assertEquals(
        "ok", actor.task(RuntimeContextTester::testRuntimeContext, actor.getId()).remote().get());
  }
}

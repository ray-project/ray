package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.job.id");
  }

  @Test
  public void testRuntimeContextInDriver() {
    Assert.assertEquals(JOB_ID, Ray.getRuntimeContext().getCurrentJobId());
  }

  public static class RuntimeContextTester {

    public String testRuntimeContext(ActorId actorId) {
      Assert.assertEquals(JOB_ID, Ray.getRuntimeContext().getCurrentJobId());
      Assert.assertEquals(actorId, Ray.getRuntimeContext().getCurrentActorId());
      return "ok";
    }
  }

  @Test
  public void testRuntimeContextInActor() {
    ActorHandle<RuntimeContextTester> actor = Ray.actor(RuntimeContextTester::new).remote();
    Assert.assertEquals(
        "ok", actor.task(RuntimeContextTester::testRuntimeContext, actor.getId()).remote().get());
  }
}

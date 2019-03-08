package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.UniqueId;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RuntimeContextTest extends BaseTest {

  private static UniqueId DRIVER_ID =
      UniqueId.fromHexString("0011223344556677889900112233445566778899");
  private static String RAYLET_SOCKET_NAME = "/tmp/ray/test/raylet_socket";
  private static String OBJECT_STORE_SOCKET_NAME = "/tmp/ray/test/object_store_socket";

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.driver.id", DRIVER_ID.toString());
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    System.setProperty("ray.object-store.socket-name", OBJECT_STORE_SOCKET_NAME);
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.driver.id");
    System.clearProperty("ray.raylet.socket-name");
    System.clearProperty("ray.object-store.socket-name");
  }

  @Test
  public void testRuntimeContextInDriver() {
    Assert.assertEquals(DRIVER_ID, Ray.getRuntimeContext().getCurrentDriverId());
    Assert.assertEquals(RAYLET_SOCKET_NAME, Ray.getRuntimeContext().getRayletSocketName());
    Assert.assertEquals(OBJECT_STORE_SOCKET_NAME,
        Ray.getRuntimeContext().getObjectStoreSocketName());
  }

  @RayRemote
  public static class RuntimeContextTester {

    public String testRuntimeContext(UniqueId actorId) {
      Assert.assertEquals(DRIVER_ID, Ray.getRuntimeContext().getCurrentDriverId());
      Assert.assertEquals(actorId, Ray.getRuntimeContext().getCurrentActorId());
      Assert.assertEquals(RAYLET_SOCKET_NAME, Ray.getRuntimeContext().getRayletSocketName());
      Assert.assertEquals(OBJECT_STORE_SOCKET_NAME,
          Ray.getRuntimeContext().getObjectStoreSocketName());
      return "ok";
    }
  }

  @Test
  public void testRuntimeContextInActor() {
    RayActor<RuntimeContextTester> actor = Ray.createActor(RuntimeContextTester::new);
    Assert.assertEquals("ok",
        Ray.call(RuntimeContextTester::testRuntimeContext, actor, actor.getId()).get());
  }

}

package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class AnonymousNamespaceTest {

  private static class A {
    public String hello() {
      return "hello";
    }
  }

  public void testIsolationBetweenAnonymousNamespaces() throws IOException, InterruptedException {
    NamespaceTest.testIsolation(
        () ->
            Assert.assertThrows(
                NoSuchElementException.class,
                () -> {
                  Ray.getGlobalActor("a").get();
                }));
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Ray.init();
    ActorHandle<A> a = Ray.actor(A::new).setGlobalName("a").remote();
    Assert.assertEquals("hello", a.task(A::hello).remote().get());
    /// Because we don't support long running job yet, so sleep to don't destroy
    /// it for a while. Otherwise the actor created in this job will be destroyed
    /// as well.
    TimeUnit.SECONDS.sleep(10);
    Ray.shutdown();
  }
}

package io.ray.docdemo;

import io.ray.api.Ray;
import org.testng.Assert;

/**
 * This class contains demo code of the Ray core Starting Ray doc (https://docs.ray.io/en/latest/starting-ray.html).
 *
 * Please keep them in sync.
 */
public class StartingRayDemo {

  public static class MyRayApp {

    public static int foo() {
      return 1;
    }

    public static void main(String[] args) {
      // This will not work because `Ray.init()` is not called.
      Ray.task(MyRayApp::foo).remote();
    }
  }

  public static void main(String[] args) {
    // TODO: throw a more specific exception here. https://github.com/ray-project/ray/issues/10436
    Assert.expectThrows(NullPointerException.class, () -> MyRayApp.main(args));
  }
}

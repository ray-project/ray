package io.ray.streaming.runtime;

import io.ray.api.Ray;
import java.lang.reflect.Method;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseRayClusterTest extends BaseUnitTest {
  @BeforeMethod
  public void beforeMethod(Method method) {
    super.beforeMethod(method);
    Ray.init();
  }

  @AfterMethod
  public void afterMethod(Method method) {
    super.afterMethod(method);
    Ray.shutdown();
  }
}

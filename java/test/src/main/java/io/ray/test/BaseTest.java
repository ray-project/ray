package io.ray.test;

import io.ray.api.Ray;
import java.lang.reflect.Method;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseTest {

  @BeforeMethod(alwaysRun = true)
  public void setUpBase(Method method) {
    Assert.assertFalse(Ray.isInitialized());
    Ray.init();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownBase() {
    Ray.shutdown();
  }
}

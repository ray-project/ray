package org.ray.api.test;

import java.lang.reflect.Method;
import org.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  @BeforeMethod
  public void setUpBase(Method method) {
    LOGGER.info("===== Running test: "
        + method.getDeclaringClass().getName() + "." + method.getName());
    System.setProperty("ray.home", "../..");
    System.setProperty("ray.resources", "CPU:4,RES-A:4");
    Ray.init();
  }

  @AfterMethod
  public void tearDownBase() {
    // TODO(qwang): This is double check to check that the socket file is removed actually.
    // We could not enable this until `systemInfo` enabled.
    //File rayletSocketFIle = new File(Ray.systemInfo().rayletSocketName());
    Ray.shutdown();

    //remove raylet socket file
    //rayletSocketFIle.delete();

    // unset system properties
    System.clearProperty("ray.home");
    System.clearProperty("ray.resources");
  }

}

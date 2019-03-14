package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import org.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  private List<String> filesToDelete;

  @BeforeMethod
  public void setUpBase(Method method) {
    LOGGER.info("===== Running test: "
        + method.getDeclaringClass().getName() + "." + method.getName());
    System.setProperty("ray.resources", "CPU:4,RES-A:4");
    Ray.init();
    // Delete socket files in tear-down.
    filesToDelete = ImmutableList.of(Ray.getRuntimeContext().getRayletSocketName(),
        Ray.getRuntimeContext().getObjectStoreSocketName());
  }

  @AfterMethod
  public void tearDownBase() {

    Ray.shutdown();

    for (String file : filesToDelete) {
      new File(file).delete();
    }

    // Unset system properties.
    System.clearProperty("ray.resources");
  }

}

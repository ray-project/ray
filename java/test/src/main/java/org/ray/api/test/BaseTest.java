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

  private List<File> filesToDelete;

  @BeforeMethod
  public void setUpBase(Method method) {
    LOGGER.info("===== Running test: "
        + method.getDeclaringClass().getName() + "." + method.getName());
    System.setProperty("ray.resources", "CPU:4,RES-A:4");
    Ray.init();
    // These files need to be deleted after each test case.
    filesToDelete = ImmutableList.of(
        new File(Ray.getRuntimeContext().getRayletSocketName()),
        new File(Ray.getRuntimeContext().getObjectStoreSocketName())
    );
    // Make sure the files will be deleted even if the test doesn't exit gracefully.
    filesToDelete.forEach(File::deleteOnExit);
  }

  @AfterMethod
  public void tearDownBase() {
    Ray.shutdown();

    for (File file : filesToDelete) {
      file.delete();
    }

    // Unset system properties.
    System.clearProperty("ray.resources");
  }

}

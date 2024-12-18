package io.ray.test;

import io.ray.api.Ray;
import java.lang.reflect.Method;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseTest {

  @BeforeMethod(alwaysRun = true)
  public void setUpBase(Method method) {
    // ray-java manages logging itself.
    // The callchain for java workload is,
    // - Java runtime creates cluster and runs C++ binaries (i.e. gcs, raylet, etc) directly;
    // - For all Java tasks/tasks, they are executed as subprocess in C++ core worker;
    // - So there's interdepenency for env variables between C++ and Java runtime;
    // - To keep the logging related env work, use a special env variable `JAVA_MANAGED_LOGGING`
    // so raylet C++ side knows don't update any logging related params themselves.
    System.setProperty("JAVA_USE_REDIRECTION", "true");
    Assert.assertFalse(Ray.isInitialized());
    Ray.init();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownBase() {
    Ray.shutdown();
  }
}

package io.ray.test;

import org.testng.IClassListener;
import org.testng.ITestClass;

public class SystemPropertyListener implements IClassListener {

  @Override
  public void onAfterClass(ITestClass testClass) {
    System.getProperties()
        .forEach(
            (k, v) -> {
              if (key.startsWith("ray.")) {
                System.clearProperty(k);
              }
            });
  }
}

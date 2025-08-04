package io.ray.test;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.testng.IClassListener;
import org.testng.ITestClass;

public class SystemPropertyListener implements IClassListener {
  private final List<String> whitelist = ImmutableList.of("ray.run-mode");

  @Override
  public void onAfterClass(ITestClass testClass) {
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("ray.")) {
        if (!whitelist.stream().anyMatch(key::equals)) {
          System.clearProperty(key);
        }
      }
    }
  }
}

package io.ray.test;

import io.ray.runtime.config.RayConfig;
import org.testng.IClassListener;
import org.testng.ITestClass;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class ConfigListener implements IClassListener, ITestListener {

  @Override
  public void onTestStart(ITestResult result) {
    RayConfig.forTestMethod();
  }

  @Override
  public void onAfterClass(ITestClass testClass) {
    RayConfig.forTestClass();
  }
}

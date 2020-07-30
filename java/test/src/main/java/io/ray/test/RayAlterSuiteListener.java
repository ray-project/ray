package io.ray.test;

import io.ray.runtime.config.RayConfig;
import io.ray.runtime.config.RunMode;
import java.util.List;
import org.testng.IAlterSuiteListener;
import org.testng.xml.XmlGroups;
import org.testng.xml.XmlRun;
import org.testng.xml.XmlSuite;

public class RayAlterSuiteListener implements IAlterSuiteListener {

  @Override
  public void alter(List<XmlSuite> suites) {
    XmlSuite suite = suites.get(0);
    String excludedGroup =
        RayConfig.create().runMode == RunMode.SINGLE_PROCESS ? "cluster" : "singleProcess";
    XmlGroups groups = new XmlGroups();
    XmlRun run = new XmlRun();
    run.onExclude(excludedGroup);
    if (!"1".equals(System.getenv("ENABLE_MULTI_LANGUAGE_TESTS"))) {
      run.onExclude("multiLanguage");
    }
    groups.setRun(run);
    suite.setGroups(groups);
  }
}
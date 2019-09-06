package org.ray.api;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.options.ActorCreationOptions;
import org.testng.TestNG;
import org.testng.xml.XmlGroups;
import org.testng.xml.XmlPackage;
import org.testng.xml.XmlRun;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

public class RunTestNG {

  public static void main(String args[]) {
    TestNG testng = new TestNG();
    XmlSuite suite = new XmlSuite();
    suite.setName("RAY suite");
    suite.setVerbose(2);
    XmlTest test = new XmlTest(suite);
    test.setName("RAY test");
    List<XmlPackage> packages = new ArrayList<>();
    packages.add(new XmlPackage("org.ray.api.test.*"));
    packages.add(new XmlPackage("org.ray.runtime.*"));
    test.setPackages(packages);
    if (ActorCreationOptions.DEFAULT_IS_DIRECT_CALL) {
      XmlGroups groups = new XmlGroups();
      XmlRun run = new XmlRun();
      run.onInclude("directCall");
      groups.setRun(run);
      test.setGroups(groups);
    }
    testng.setXmlSuites(ImmutableList.of(suite));
    testng.setOutputDirectory("/tmp/ray_java_test_output");
    testng.run();
    System.exit(testng.getStatus());
  }
}

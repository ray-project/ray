package org.ray.api;

import java.util.List;
import org.ray.api.options.ActorCreationOptions;
import org.testng.IAlterSuiteListener;
import org.testng.xml.XmlGroups;
import org.testng.xml.XmlRun;
import org.testng.xml.XmlSuite;

public class RayAlterSuiteListener implements IAlterSuiteListener {

  @Override
  public void alter(List<XmlSuite> suites) {
    XmlSuite suite = suites.get(0);
    if (ActorCreationOptions.DEFAULT_USE_DIRECT_CALL) {
      XmlGroups groups = new XmlGroups();
      XmlRun run = new XmlRun();
      run.onInclude("directCall");
      groups.setRun(run);
      suite.setGroups(groups);
    }
  }
}

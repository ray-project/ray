package org.ray.api.test;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

public class MyRunner extends BlockJUnit4ClassRunner {

  public MyRunner(Class<?> klass) throws InitializationError {
    super(klass);
  }

  @Override
  public void run(RunNotifier notifier) {
    notifier.addListener(new TestListener());
    notifier.fireTestRunStarted(getDescription());
    super.run(notifier);
  }
}

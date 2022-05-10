package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.PendingCallsLimitExceededException;
import io.ray.api.id.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class BackPressureTest extends BaseTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackPressureTest.class);

  @BeforeClass
  public void setupJobConfig() {}

  private static final ObjectId objectId = ObjectId.fromRandom();

  public static String unblockSignalActor(ActorHandle<SignalActor> signal) {
    signal.task(SignalActor::sendSignal).remote().get();
    return null;
  }

  public void testBackPressure() {
    /// set max concurrency to 11, 10 of them for executing waitSignal, and 1
    /// of them for executing sendSignal.
    ActorHandle<SignalActor> signalActor =
        Ray.actor(SignalActor::new).setMaxConcurrency(11).setMaxPendingCalls(10).remote();
    /// Ping the actor to insure the actor is alive already.
    signalActor.task(SignalActor::ping).remote().get();

    for (int i = 0; i < 10; i++) {
      LOGGER.info("call waitSignal");
      Assert.assertNotNull(signalActor.task(SignalActor::waitSignal).remote());
    }

    // Check backpressure occur.
    boolean backPressure = false;
    try {
      LOGGER.info("call waitSignal");
      signalActor.task(SignalActor::waitSignal).remote();
    } catch (PendingCallsLimitExceededException e) {
      LOGGER.info(e.toString());
      backPressure = true;
    } finally {
      Assert.assertTrue(backPressure);
    }

    // Unblock signal actor, to make all backpressured raycall executed.
    for (int i = 0; i < 10; i++) {
      Ray.task(BackPressureTest::unblockSignalActor, signalActor).remote().get();
    }

    // Check the raycall is normal
    signalActor.task(SignalActor::ping).remote().get();
  }
}

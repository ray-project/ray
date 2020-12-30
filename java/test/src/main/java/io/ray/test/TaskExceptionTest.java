package io.ray.test;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;
public class TaskExceptionTest extends BaseTest {
  private static class UnserializableClass {}
  private static class UnserializableException extends RuntimeException {
    public UnserializableException() {
      super();
    }
    private UnserializableClass unSerializableClass = new UnserializableClass();
  }
  private static class MyActor {
    public String sayHi() {
      return "Hi";
    }
    public String throwUnserializableException() {
      throw new UnserializableException();
    }
  }
  private static String throwUnserializableException() {
    throw new UnserializableException();
  }
  @Test
  public void testThrowUnserializableExceptionInNormalTask() {
    // Test that if a task throws an unserializable exception, the worker won't crash.
    assertThrowingSpecifiedException((() -> Ray.task(TaskExceptionTest::throwUnserializableException).remote().get()));
  }

  @Test
  public void testThrowUnserializableExceptionInActorTask() {
    ActorHandle<MyActor> myActor = Ray.actor(MyActor::new).remote();
    Assert.assertEquals("Hi", myActor.task(MyActor::sayHi).remote().get());
    assertThrowingSpecifiedException((() -> myActor.task(MyActor::throwUnserializableException).remote().get()));
  }
  private static void assertThrowingSpecifiedException(Assert.ThrowingRunnable func) {
    Exception throwingException = null;
    Assert.assertThrows(func);
  }
}

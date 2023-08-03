package io.ray.serve.poll;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyListenerTest {

  @Test
  public void test() throws Throwable {
    int[] a = new int[] {0};
    KeyListener keyListener = (x) -> ((int[]) x)[0] = 1;
    keyListener.notifyChanged(a);
    Assert.assertEquals(a[0], 1);
  }
}

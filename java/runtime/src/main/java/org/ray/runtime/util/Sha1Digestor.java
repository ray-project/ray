package org.ray.runtime.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import org.ray.runtime.util.logger.RayLog;

public class Sha1Digestor {

  private static final ThreadLocal<MessageDigest> md = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA1");
    } catch (Exception e) {
      RayLog.core.error("Cannot get SHA1 MessageDigest", e);
      throw new RuntimeException("Cannot get SHA1 digest", e);
    }
  });


  private static final ThreadLocal<ByteBuffer> longBuffer = ThreadLocal
      .withInitial(() -> ByteBuffer.allocate(Long.SIZE / Byte.SIZE));

  public static byte[] digest(byte[] src, long addIndex) {
    MessageDigest dg = md.get();
    longBuffer.get().clear();
    dg.reset();

    dg.update(src);
    dg.update(longBuffer.get().putLong(addIndex).array());
    return dg.digest();
  }

  public static byte[] digest(String str) {
    return digest(str.getBytes(StringUtil.UTF8));
  }

  public static byte[] digest(byte[] src) {
    MessageDigest dg = md.get();
    dg.reset();
    return dg.digest(src);
  }
}
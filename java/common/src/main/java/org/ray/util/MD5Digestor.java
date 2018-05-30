package org.ray.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import org.ray.util.logger.RayLog;

public class MD5Digestor {

  private static final ThreadLocal<MessageDigest> md = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (Exception e) {
      RayLog.core.error("cannot get MD5 MessageDigest", e);
      throw new RuntimeException("cannot get MD5 digest", e);
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
}

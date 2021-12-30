package io.ray.runtime.serialization.serializers;

import com.google.common.base.Preconditions;
import io.ray.runtime.io.Platform;
import io.ray.runtime.serialization.util.ReflectionUtils;
import java.lang.reflect.Field;
import sun.misc.Unsafe;

/** An object field accessor based on {@link Unsafe}. */
public class UnsafeFieldAccessor {
  private final Field field;
  private final long fieldOffset;

  /**
   * Search parent class if <code>cls</code> doesn't have a field named <code>fieldName</code>.
   *
   * @param cls class
   * @param fieldName field name
   */
  public UnsafeFieldAccessor(Class<?> cls, String fieldName) {
    this(ReflectionUtils.getField(cls, fieldName));
  }

  public UnsafeFieldAccessor(Field field) {
    Preconditions.checkNotNull(field);
    this.field = field;
    this.fieldOffset = Platform.UNSAFE.objectFieldOffset(field);
    Preconditions.checkArgument(fieldOffset != -1);
  }

  public Field getField() {
    return field;
  }

  public boolean getBoolean(Object obj) {
    return Platform.UNSAFE.getBoolean(obj, fieldOffset);
  }

  public void putBoolean(Object obj, boolean value) {
    Platform.UNSAFE.putBoolean(obj, fieldOffset, value);
  }

  public byte getByte(Object obj) {
    return Platform.UNSAFE.getByte(obj, fieldOffset);
  }

  public void putByte(Object obj, byte value) {
    Platform.UNSAFE.putByte(obj, fieldOffset, value);
  }

  public char getChar(Object obj) {
    return Platform.UNSAFE.getChar(obj, fieldOffset);
  }

  public void putChar(Object obj, char value) {
    Platform.UNSAFE.putChar(obj, fieldOffset, value);
  }

  public short getShort(Object obj) {
    return Platform.UNSAFE.getShort(obj, fieldOffset);
  }

  public void putShort(Object obj, short value) {
    Platform.UNSAFE.putShort(obj, fieldOffset, value);
  }

  public int getInt(Object obj) {
    return Platform.UNSAFE.getInt(obj, fieldOffset);
  }

  public void putInt(Object obj, int value) {
    Platform.UNSAFE.putInt(obj, fieldOffset, value);
  }

  public long getLong(Object obj) {
    return Platform.UNSAFE.getLong(obj, fieldOffset);
  }

  public void putLong(Object obj, long value) {
    Platform.UNSAFE.putLong(obj, fieldOffset, value);
  }

  public float getFloat(Object obj) {
    return Platform.UNSAFE.getFloat(obj, fieldOffset);
  }

  public void putFloat(Object obj, float value) {
    Platform.UNSAFE.putFloat(obj, fieldOffset, value);
  }

  public double getDouble(Object obj) {
    return Platform.UNSAFE.getDouble(obj, fieldOffset);
  }

  public void putDouble(Object obj, double value) {
    Platform.UNSAFE.putDouble(obj, fieldOffset, value);
  }

  public Object getObject(Object obj) {
    return Platform.UNSAFE.getObject(obj, fieldOffset);
  }

  public void putObject(Object obj, Object value) {
    Platform.UNSAFE.putObject(obj, fieldOffset, value);
  }
}

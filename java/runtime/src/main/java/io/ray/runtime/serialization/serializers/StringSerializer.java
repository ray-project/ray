package io.ray.runtime.serialization.serializers;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.io.Platform;
import io.ray.runtime.serialization.RaySerde;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

public final class StringSerializer extends Serializer<String> {
  private static final boolean STRING_VALUE_FIELD_IS_CHARS;
  private static final long STRING_VALUE_FIELD_OFFSET;
  private static final long STRING_OFFSET_FIELD_OFFSET;
  private static final long STRING_COUNT_FIELD_OFFSET;
  private static final Constructor<String> STRING_NO_COPY_CONSTRUCTOR;
  private static final long USE_NO_COPY_CONSTRUCTOR_THRESHOLD;

  static {
    Field valueField = getField("value");
    // TODO add zero-copy serialization for jdk11+ string
    STRING_VALUE_FIELD_IS_CHARS = valueField != null && valueField.getType() == char[].class;
    STRING_VALUE_FIELD_OFFSET = getFieldOffset("value");
    STRING_OFFSET_FIELD_OFFSET = getFieldOffset("offset");
    STRING_COUNT_FIELD_OFFSET = getFieldOffset("count");
    Constructor<String> ctr = null;
    try {
      ctr = String.class.getDeclaredConstructor(char[].class, boolean.class);
      ctr.setAccessible(true);
    } catch (NoSuchMethodException ignored) {
      // ignore.
    }
    STRING_NO_COPY_CONSTRUCTOR = ctr;
    if (STRING_NO_COPY_CONSTRUCTOR == null) {
      // If String.class doesn't have nocopy constructor,
      // set THRESHOLD to max to skip copy constructor.
      USE_NO_COPY_CONSTRUCTOR_THRESHOLD = Long.MAX_VALUE;
    } else {
      // benchmark shows that cost of call constructor by reflection is less than char array copy
      // when string size is grater than 10.
      USE_NO_COPY_CONSTRUCTOR_THRESHOLD = 10;
    }
  }

  private static long getFieldOffset(String fieldName) {
    Field field = getField(fieldName);
    return field == null ? -1 : Platform.UNSAFE.objectFieldOffset(field);
  }

  private static Field getField(String fieldName) {
    try {
      return String.class.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      // field undefined
    }
    return null;
  }

  public StringSerializer(RaySerde raySerDe) {
    super(raySerDe, String.class);
  }

  @Override
  public void write(MemoryBuffer buffer, String value) {
    writeJavaString(buffer, value);
  }

  @Override
  public String read(MemoryBuffer buffer) {
    return readJavaString(buffer);
  }

  private char[] charArray = null;

  public void writeJavaString(MemoryBuffer buffer, String value) {
    // based on https://stackoverflow.com/questions/19692206/writing-and-reading-strings-with-unsafe
    final int strLen = value.length();
    if (strLen == 0) {
      buffer.writeInt(0);
    } else {
      if (STRING_VALUE_FIELD_IS_CHARS) {
        final char[] chars = (char[]) Platform.UNSAFE.getObject(value, STRING_VALUE_FIELD_OFFSET);
        if (STRING_OFFSET_FIELD_OFFSET != -1 && STRING_COUNT_FIELD_OFFSET != -1) {
          final int offset = Platform.UNSAFE.getInt(value, STRING_OFFSET_FIELD_OFFSET);
          Serializers.writePrimitiveArray(
              buffer, chars, Platform.CHAR_ARRAY_OFFSET + 2 * offset, strLen, 2);
        } else {
          Serializers.writePrimitiveArray(buffer, chars, Platform.CHAR_ARRAY_OFFSET, strLen, 2);
        }
      } else {
        char[] chars = getCharArray(strLen);
        value.getChars(0, strLen, chars, 0);
        Serializers.writePrimitiveArray(buffer, chars, Platform.CHAR_ARRAY_OFFSET, strLen, 2);
      }
    }
  }

  private char[] getCharArray(int numElements) {
    if (charArray == null || charArray.length < numElements) {
      charArray = new char[numElements];
    }
    return charArray;
  }

  // We don't use UNSAFE.putObject to write String value field, because it's final, write final will
  // violate
  // java final semantics. See
  // https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#d5e34106
  // JavaLangAccess is used by jdb, so we dont's use it.
  public String readJavaString(MemoryBuffer buffer) {
    final int size = buffer.readInt();
    if (size == 0) {
      return "";
    } else {
      final int numElements = size / 2;
      final boolean useNoCopyCtr = numElements > USE_NO_COPY_CONSTRUCTOR_THRESHOLD;
      char[] chars;
      if (useNoCopyCtr) {
        chars = new char[numElements];
      } else {
        chars = getCharArray(numElements);
      }
      int readerIndex = buffer.readerIndex();
      buffer.copyToUnsafe(readerIndex, chars, Platform.CHAR_ARRAY_OFFSET, size);
      buffer.readerIndex(readerIndex + size);
      if (useNoCopyCtr) {
        try {
          return STRING_NO_COPY_CONSTRUCTOR.newInstance(chars, true);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      } else {
        return new String(chars, 0, numElements);
      }
    }
  }
}

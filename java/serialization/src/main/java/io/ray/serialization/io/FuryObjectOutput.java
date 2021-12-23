package io.ray.serialization.io;

import com.google.common.base.Preconditions;
import io.ray.serialization.Fury;
import io.ray.serialization.serializers.StringSerializer;
import io.ray.serialization.util.MemoryBuffer;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;

public class FuryObjectOutput extends OutputStream implements ObjectOutput {
  private final Fury fury;
  private final DataOutputStream utf8out = new DataOutputStream(this);
  private final StringSerializer stringSerializer;
  private MemoryBuffer buffer;

  public FuryObjectOutput(Fury fury, MemoryBuffer buffer) {
    this.fury = fury;
    this.buffer = buffer;
    this.stringSerializer = new StringSerializer(fury);
  }

  public MemoryBuffer getBuffer() {
    return buffer;
  }

  public void setBuffer(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public void writeObject(Object obj) throws IOException {
    fury.serializeReferencableToJava(buffer, obj);
  }

  @Override
  public void write(int b) throws IOException {
    buffer.writeByte((byte) b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    buffer.writeBytes(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    buffer.writeBytes(b, off, len);
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    buffer.writeBoolean(v);
  }

  @Override
  public void writeByte(int v) throws IOException {
    buffer.writeByte((byte) v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    buffer.writeShort((short) v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    buffer.writeChar((char) v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    buffer.writeInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    buffer.writeLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    buffer.writeFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    buffer.writeDouble(v);
  }

  @Override
  public void writeBytes(String s) throws IOException {
    Preconditions.checkNotNull(s);
    int len = s.length();
    for (int i = 0; i < len; i++) {
      buffer.writeByte((byte) s.charAt(i));
    }
  }

  @Override
  public void writeChars(String s) throws IOException {
    Preconditions.checkNotNull(s);
    stringSerializer.writeJavaString(buffer, s);
  }

  @Override
  public void writeUTF(String s) throws IOException {
    Preconditions.checkNotNull(s);
    stringSerializer.writeJavaString(buffer, s);
  }

  @Override
  public void flush() throws IOException {}

  @Override
  public void close() throws IOException {}
}

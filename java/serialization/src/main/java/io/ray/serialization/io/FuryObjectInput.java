package io.ray.serialization.io;

import com.google.common.base.Preconditions;
import io.ray.serialization.Fury;
import io.ray.serialization.serializers.StringSerializer;
import io.ray.serialization.util.MemoryBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;

public class FuryObjectInput extends InputStream implements ObjectInput {
  private final Fury fury;
  private MemoryBuffer buffer;
  private final StringSerializer stringSerializer;

  public FuryObjectInput(Fury fury, MemoryBuffer buffer) {
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
  public Object readObject() throws ClassNotFoundException, IOException {
    return fury.deserializeReferencableFromJava(buffer);
  }

  @Override
  public int read() throws IOException {
    return buffer.readByte();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int available = available();
    if (available == 0) {
      return -1;
    }

    len = Math.min(available, len);
    buffer.readBytes(b, off, len);
    return len;
  }

  @Override
  public long skip(long n) throws IOException {
    Preconditions.checkArgument(n < Integer.MAX_VALUE);
    int newIndex = Math.addExact(buffer.readerIndex(), (int) n);
    Preconditions.checkArgument(newIndex < buffer.size());
    buffer.readerIndex(newIndex);
    return n;
  }

  @Override
  public int available() throws IOException {
    return buffer.size() - buffer.readerIndex();
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    buffer.readBytes(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    buffer.readerIndex(buffer.readerIndex() + n);
    return n;
  }

  @Override
  public boolean readBoolean() throws IOException {
    return buffer.readBoolean();
  }

  @Override
  public byte readByte() throws IOException {
    return buffer.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return buffer.readByte() & 0xFF;
  }

  @Override
  public short readShort() throws IOException {
    return buffer.readShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return buffer.readShort() & 0xffff;
  }

  @Override
  public char readChar() throws IOException {
    return buffer.readChar();
  }

  @Override
  public int readInt() throws IOException {
    return buffer.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return buffer.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return buffer.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return buffer.readDouble();
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    return stringSerializer.readJavaString(buffer);
  }
}

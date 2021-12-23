package io.ray.serialization.types;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;

public class DateTimeUtils {
  public static long MICROS_PER_SECOND = SECONDS.toMicros(1);
  public static final long NANOS_PER_MICROS = MICROSECONDS.toNanos(1);

  public static int localDateToDays(LocalDate localDate) {
    return Math.toIntExact(localDate.toEpochDay());
  }

  public static long fromJavaTimestamp(Timestamp t) {
    return instantToMicros(t.toInstant());
  }

  public static long instantToMicros(Instant instant) {
    long us = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);
    return Math.addExact(us, NANOSECONDS.toMicros(instant.getNano()));
  }

  public static LocalDate daysToLocalDate(int days) {
    return LocalDate.ofEpochDay(days);
  }

  public static Instant microsToInstant(long us) {
    long secs = Math.floorDiv(us, MICROS_PER_SECOND);
    long mos = Math.floorMod(us, MICROS_PER_SECOND);
    return Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS);
  }

  public static Timestamp toJavaTimestamp(long us) {
    return Timestamp.from(microsToInstant(us));
  }
}

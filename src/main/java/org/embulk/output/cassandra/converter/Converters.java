package org.embulk.output.cassandra.converter;

public class Converters {
  public static final StringConverter STRING = new StringConverter();
  public static final BigintConverter BIGINT = new BigintConverter();
  public static final BooleanConverter BOOLEAN = new BooleanConverter();
  public static final DecimalConverter DECIMAL = new DecimalConverter();
  public static final DoubleConverter DOUBLE = new DoubleConverter();
  public static final FloatConverter FLOAT = new FloatConverter();
  public static final InetConverter INET = new InetConverter();
  public static final IntConverter INT = new IntConverter();
  public static final SmallintConverter SMALLINT = new SmallintConverter();
  public static final TimestampConverter TIMESTAMP = new TimestampConverter();
  public static final TimeuuidConverter TIMEUUID = new TimeuuidConverter();
  public static final TinyintConverter TINYINT = new TinyintConverter();
  public static final UuidConverter UUID = new UuidConverter();
  public static final VarintConverter VARINT = new VarintConverter();
}

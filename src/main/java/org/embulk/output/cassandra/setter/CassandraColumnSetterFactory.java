package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.protocol.internal.ProtocolConstants;

public class CassandraColumnSetterFactory {
  private CassandraColumnSetterFactory() {}

  public static CassandraColumnSetter createColumnSetter(ColumnMetadata cassandraColumn) {
    DataType type = cassandraColumn.getType();
    if (type.equals(DataTypes.TEXT) || type.equals(DataTypes.ASCII)) {
      return new TextColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.BOOLEAN)) {
      return new BooleanColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.BIGINT) || type.equals(DataTypes.COUNTER)) {
      return new BigintColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.INT)) {
      return new IntColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.SMALLINT)) {
      return new SmallintColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.TINYINT)) {
      return new TinyintColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.VARINT)) {
      return new VarintColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.DECIMAL)) {
      return new DecimalColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.DOUBLE)) {
      return new DoubleColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.FLOAT)) {
      return new FloatColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.TIMESTAMP)) {
      return new TimestampColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.DATE)) {
      return new DateColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.TIME)) {
      return new TimeColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.DURATION)) {
      return new DurationColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.UUID)) {
      return new UuidColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.TIMEUUID)) {
      return new TimeuuidColumnSetter(cassandraColumn);
    } else if (type.equals(DataTypes.INET)) {
      return new InetColumnSetter(cassandraColumn);
    }

    if (cassandraColumn.getType().getProtocolCode() == ProtocolConstants.DataType.LIST) {
      return new ListColumnSetter(cassandraColumn);
    } else if (cassandraColumn.getType().getProtocolCode() == ProtocolConstants.DataType.SET) {
      return new SetColumnSetter(cassandraColumn);
    } else if (cassandraColumn.getType().getProtocolCode() == ProtocolConstants.DataType.MAP) {
      return new MapColumnSetter(cassandraColumn);
    } else if (cassandraColumn.getType().getProtocolCode() == ProtocolConstants.DataType.TUPLE) {
      return new TupleColumnSetter(cassandraColumn);
    } else {
      throw new RuntimeException(
          cassandraColumn.getName() + ":" + cassandraColumn.getType() + " is unsupported Type");
    }
  }
}

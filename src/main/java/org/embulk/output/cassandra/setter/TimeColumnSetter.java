package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeColumnSetter extends CassandraColumnSetter {
  public TimeColumnSetter(ColumnMetadata cassandraColumn) {
    super(cassandraColumn);
  }

  @Override
  public void setLongValue(Long value, BoundStatementBuilder statement) {
    statement.setLocalTime(cassandraColumn.getName(), LocalTime.ofNanoOfDay(value));
  }

  @Override
  public void setDoubleValue(Double value, BoundStatementBuilder statement) {
    statement.setLocalTime(cassandraColumn.getName(), LocalTime.ofNanoOfDay(value.longValue()));
  }

  @Override
  public void setStringValue(String value, BoundStatementBuilder statement) {
    LocalTime time = LocalTime.parse(value);
    statement.setLocalTime(cassandraColumn.getName(), time);
  }

  @Override
  public void setTimestampValue(Instant value, BoundStatementBuilder statement) {
    ZonedDateTime datetime = ZonedDateTime.ofInstant(value, ZoneId.of("UTC"));
    statement.setLocalTime(cassandraColumn.getName(), datetime.toLocalTime());
  }
}

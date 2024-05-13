package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.time.Instant;

public class TimestampColumnSetter extends CassandraColumnSetter {
  public TimestampColumnSetter(ColumnMetadata cassandraColumn) {
    super(cassandraColumn);
  }

  @Override
  public void setLongValue(Long value, BoundStatementBuilder statement) {
    statement.setInstant(cassandraColumn.getName(), Instant.ofEpochMilli(value));
  }

  @Override
  public void setDoubleValue(Double value, BoundStatementBuilder statement) {
    long longValue = value.longValue();
    statement.setInstant(cassandraColumn.getName(), Instant.ofEpochMilli(longValue));
  }

  @Override
  public void setStringValue(String value, BoundStatementBuilder statement) {
    statement.setInstant(cassandraColumn.getName(), Instant.parse(value));
  }

  @Override
  public void setTimestampValue(Instant value, BoundStatementBuilder statement) {
    statement.setInstant(cassandraColumn.getName(), value);
  }
}

package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class BigintColumnSetter extends CassandraColumnSetter {
  public BigintColumnSetter(ColumnMetadata cassandraColumn) {
    super(cassandraColumn);
  }

  @Override
  public void setBooleanValue(Boolean value, BoundStatementBuilder statement) {
    if (value) {
      statement.setLong(cassandraColumn.getName(), 1);
    } else {
      statement.setLong(cassandraColumn.getName(), 0);
    }
  }

  @Override
  public void setLongValue(Long value, BoundStatementBuilder statement) {
    statement.setLong(cassandraColumn.getName(), value);
  }

  @Override
  public void setDoubleValue(Double value, BoundStatementBuilder statement) {
    statement.setLong(cassandraColumn.getName(), value.longValue());
  }

  @Override
  public void setStringValue(String value, BoundStatementBuilder statement) {
    statement.setLong(cassandraColumn.getName(), Long.parseLong(value));
  }
}

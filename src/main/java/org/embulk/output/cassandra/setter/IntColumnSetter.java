package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class IntColumnSetter extends CassandraColumnSetter {
  public IntColumnSetter(ColumnMetadata cassandraColumn) {
    super(cassandraColumn);
  }

  @Override
  public void setBooleanValue(Boolean value, BoundStatementBuilder statement) {
    if (value) {
      statement.setInt(cassandraColumn.getName(), 1);
    } else {
      statement.setInt(cassandraColumn.getName(), 0);
    }
  }

  @Override
  public void setLongValue(Long value, BoundStatementBuilder statement) {
    statement.setInt(cassandraColumn.getName(), value.intValue());
  }

  @Override
  public void setDoubleValue(Double value, BoundStatementBuilder statement) {
    statement.setInt(cassandraColumn.getName(), value.intValue());
  }

  @Override
  public void setStringValue(String value, BoundStatementBuilder statement) {
    statement.setInt(cassandraColumn.getName(), Integer.parseInt(value));
  }
}

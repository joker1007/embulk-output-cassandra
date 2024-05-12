package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class DurationColumnSetter extends CassandraColumnSetter {
  public DurationColumnSetter(ColumnMetadata cassandraColumn) {
    super(cassandraColumn);
  }

  @Override
  public void setStringValue(String value, BoundStatementBuilder statement) {
    CqlDuration duration = CqlDuration.from(value);
    statement.setCqlDuration(cassandraColumn.getName(), duration);
  }
}

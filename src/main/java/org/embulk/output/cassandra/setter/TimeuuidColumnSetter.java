package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.uuid.Uuids;

public class TimeuuidColumnSetter extends CassandraColumnSetter {
  public TimeuuidColumnSetter(ColumnMetadata cassandraColumn) {
    super(cassandraColumn);
  }

  @Override
  public void setNullValue(BoundStatementBuilder statement) {
    statement.setUuid(cassandraColumn.getName(), Uuids.timeBased());
  }
}

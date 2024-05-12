package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.time.Instant;
import org.embulk.spi.json.JsonValue;

public class TextColumnSetter extends CassandraColumnSetter {
  public TextColumnSetter(ColumnMetadata cassandraColumn) {
    super(cassandraColumn);
  }

  @Override
  public void setBooleanValue(Boolean value, BoundStatementBuilder statement) {
    statement.setString(cassandraColumn.getName(), value.toString());
  }

  @Override
  public void setLongValue(Long value, BoundStatementBuilder statement) {
    statement.setString(cassandraColumn.getName(), value.toString());
  }

  @Override
  public void setDoubleValue(Double value, BoundStatementBuilder statement) {
    statement.setString(cassandraColumn.getName(), value.toString());
  }

  @Override
  public void setStringValue(String value, BoundStatementBuilder statement) {
    statement.setString(cassandraColumn.getName(), value);
  }

  @Override
  public void setTimestampValue(Instant value, BoundStatementBuilder statement) {
    statement.setString(cassandraColumn.getName(), value.toString());
  }

  @Override
  public void setJsonValue(JsonValue value, BoundStatementBuilder statement) {
    statement.setString(cassandraColumn.getName(), value.toJson());
  }
}

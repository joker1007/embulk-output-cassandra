package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.math.BigDecimal;

public class DecimalColumnSetter extends CassandraColumnSetter {
  public DecimalColumnSetter(ColumnMetadata cassandraColumn) {
    super(cassandraColumn);
  }

  @Override
  public void setBooleanValue(Boolean value, BoundStatementBuilder statement) {
    if (value) {
      statement.setBigDecimal(cassandraColumn.getName(), BigDecimal.valueOf(1));
    } else {
      statement.setBigDecimal(cassandraColumn.getName(), BigDecimal.valueOf(0));
    }
  }

  @Override
  public void setLongValue(Long value, BoundStatementBuilder statement) {
    statement.setBigDecimal(cassandraColumn.getName(), BigDecimal.valueOf(value));
  }

  @Override
  public void setDoubleValue(Double value, BoundStatementBuilder statement) {
    statement.setBigDecimal(cassandraColumn.getName(), BigDecimal.valueOf(value));
  }

  @Override
  public void setStringValue(String value, BoundStatementBuilder statement) {
    statement.setBigDecimal(cassandraColumn.getName(), new BigDecimal(value));
  }
}

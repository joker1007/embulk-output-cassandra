package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import org.embulk.spi.json.JsonValue;

import java.time.Instant;

public abstract class CassandraColumnSetter
{
    protected ColumnMetadata cassandraColumn;

    public CassandraColumnSetter(ColumnMetadata cassandraColumn)
    {
        this.cassandraColumn = cassandraColumn;
    }

    public void setBooleanValue(Boolean value, BoundStatementBuilder statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getName());
    };

    public void setLongValue(Long value, BoundStatementBuilder statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getName());
    };

    public void setDoubleValue(Double value, BoundStatementBuilder statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getName());
    };

    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getName());
    };

    public void setTimestampValue(Instant value, BoundStatementBuilder statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getName());
    };

    public void setJsonValue(JsonValue value, BoundStatementBuilder statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getName());
    };

    public void setNullValue(BoundStatementBuilder statement)
    {
        statement.setToNull(cassandraColumn.getName());
    }
}

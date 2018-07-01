package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public abstract class CassandraColumnSetter
{
    protected ColumnMetadata cassandraColumn;

    public CassandraColumnSetter(ColumnMetadata cassandraColumn)
    {
        this.cassandraColumn = cassandraColumn;
    }

    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getType().getName());
    };

    public void setLongValue(Long value, BoundStatement statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getType().getName());
    };

    public void setDoubleValue(Double value, BoundStatement statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getType().getName());
    };

    public void setStringValue(String value, BoundStatement statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getType().getName());
    };

    public void setTimestampValue(Timestamp value, BoundStatement statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getType().getName());
    };

    public void setJsonValue(Value value, BoundStatement statement)
    {
        throw new RuntimeException("Unsupported type conversion: " + value.getClass().getName() + " -> " + cassandraColumn.getType().getName());
    };

    public void setNullValue(BoundStatement statement)
    {
        statement.setToNull(cassandraColumn.getName());
    }
}

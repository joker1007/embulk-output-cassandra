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

    public abstract void setBooleanValue(Boolean value, BoundStatement statement);
    public abstract void setLongValue(Long value, BoundStatement statement);
    public abstract void setDoubleValue(Double value, BoundStatement statement);
    public abstract void setStringValue(String value, BoundStatement statement);
    public abstract void setTimestampValue(Timestamp value, BoundStatement statement);
    public abstract void setJsonValue(Value value, BoundStatement statement);

    public void setNullValue(BoundStatement statement)
    {
        statement.setToNull(cassandraColumn.getName());
    }
}

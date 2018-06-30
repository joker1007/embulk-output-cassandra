package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public class TextColumnSetter extends CassandraColumnSetter
{

    public TextColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatement statement)
    {
        statement.setString(cassandraColumn.getName(), value.toString());
    }

    @Override
    public void setLongValue(Long value, BoundStatement statement)
    {
        statement.setString(cassandraColumn.getName(), value.toString());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatement statement)
    {
        statement.setString(cassandraColumn.getName(), value.toString());
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        statement.setString(cassandraColumn.getName(), value);
    }

    @Override
    public void setTimestampValue(Timestamp value, BoundStatement statement)
    {
        statement.setString(cassandraColumn.getName(), value.toString());
    }

    @Override
    public void setJsonValue(Value value, BoundStatement statement)
    {
        statement.setString(cassandraColumn.getName(), value.toJson());
    }
}

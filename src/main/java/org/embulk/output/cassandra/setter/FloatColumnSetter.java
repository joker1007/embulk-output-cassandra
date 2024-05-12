package org.embulk.output.cassandra.setter;


import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class FloatColumnSetter extends CassandraColumnSetter
{
    public FloatColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatementBuilder statement)
    {
        if (value) {
            statement.setFloat(cassandraColumn.getName(), 1);
        }
        else {
            statement.setFloat(cassandraColumn.getName(), 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatementBuilder statement)
    {
        statement.setFloat(cassandraColumn.getName(), value.floatValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatementBuilder statement)
    {
        statement.setFloat(cassandraColumn.getName(), value.floatValue());
    }

    @Override
    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        statement.setFloat(cassandraColumn.getName(), Float.parseFloat(value));
    }
}

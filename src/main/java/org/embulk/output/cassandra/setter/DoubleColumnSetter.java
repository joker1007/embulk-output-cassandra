package org.embulk.output.cassandra.setter;


import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class DoubleColumnSetter extends CassandraColumnSetter
{
    public DoubleColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatementBuilder statement)
    {
        if (value) {
            statement.setDouble(cassandraColumn.getName(), 1);
        }
        else {
            statement.setDouble(cassandraColumn.getName(), 0);
        }
    }

    @Override
    public void setLongValue(Long value, BoundStatementBuilder statement)
    {
        statement.setDouble(cassandraColumn.getName(), value.doubleValue());
    }

    @Override
    public void setDoubleValue(Double value, BoundStatementBuilder statement)
    {
        statement.setDouble(cassandraColumn.getName(), value);
    }

    @Override
    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        statement.setDouble(cassandraColumn.getName(), Double.parseDouble(value));
    }
}

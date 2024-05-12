package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class BooleanColumnSetter extends CassandraColumnSetter
{
    public BooleanColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setBooleanValue(Boolean value, BoundStatementBuilder statement)
    {
        statement.setBool(cassandraColumn.getName(), value);
    }

    @Override
    public void setLongValue(Long value, BoundStatementBuilder statement)
    {
        statement.setBool(cassandraColumn.getName(), value != 0);
    }

    @Override
    public void setDoubleValue(Double value, BoundStatementBuilder statement)
    {
        statement.setBool(cassandraColumn.getName(), value != 0);
    }
}

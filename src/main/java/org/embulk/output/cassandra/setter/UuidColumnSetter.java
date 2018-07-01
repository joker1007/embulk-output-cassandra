package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.utils.UUIDs;

public class UuidColumnSetter extends CassandraColumnSetter
{
    public UuidColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    public void setNullValue(BoundStatement statement)
    {
        statement.setUUID(cassandraColumn.getName(), UUIDs.random());
    }
}

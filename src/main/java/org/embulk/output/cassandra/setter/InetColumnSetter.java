package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class InetColumnSetter extends CassandraColumnSetter
{
    public InetColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setStringValue(String value, BoundStatementBuilder statement)
    {
        try {
            statement.setInetAddress(cassandraColumn.getName(), InetAddress.getByName(value));
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}

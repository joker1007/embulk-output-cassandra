package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class InetColumnSetter extends CassandraColumnSetter
{
    public InetColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setStringValue(String value, BoundStatement statement)
    {
        try {
            statement.setInet(cassandraColumn.getName(), InetAddress.getByName(value));
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}

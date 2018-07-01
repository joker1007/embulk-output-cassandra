package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.msgpack.value.Value;

public class ListColumnSetter extends CassandraColumnSetter
{

    public ListColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setJsonValue(Value value, BoundStatement statement)
    {
        if (!value.isArrayValue()) {
            throw new RuntimeException(value.toJson() + " is not array value");
        }
        statement.setList(cassandraColumn.getName(), ValueConverter.convertList(value.asArrayValue().list()));
    }
}

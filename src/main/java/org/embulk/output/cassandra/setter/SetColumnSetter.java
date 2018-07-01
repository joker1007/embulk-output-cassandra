package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import org.msgpack.value.Value;

import java.util.LinkedHashSet;

public class SetColumnSetter extends CassandraColumnSetter
{

    public SetColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setJsonValue(Value value, BoundStatement statement)
    {
        if (!value.isArrayValue()) {
            throw new RuntimeException(value.toJson() + " is not array value");
        }
        statement.setSet(cassandraColumn.getName(), new LinkedHashSet<>(ValueConverter.convertList(value.asArrayValue().list())));
    }
}

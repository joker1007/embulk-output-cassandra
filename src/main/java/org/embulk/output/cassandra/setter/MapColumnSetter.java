package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.google.common.collect.Maps;
import org.msgpack.value.Value;

import java.util.Map;
import java.util.stream.Collectors;

public class MapColumnSetter extends CassandraColumnSetter
{

    public MapColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setJsonValue(Value value, BoundStatement statement)
    {
        if (!value.isMapValue()) {
            throw new RuntimeException(value.toJson() + " is not map value");
        }
        statement.setMap(cassandraColumn.getName(), ValueConverter.convertMap(value.asMapValue().map()));
    }
}

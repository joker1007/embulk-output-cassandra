package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.embulk.output.cassandra.converter.ValueConverter;
import org.embulk.spi.json.JsonValue;

public class MapColumnSetter extends CassandraColumnSetter
{
    public MapColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setJsonValue(JsonValue value, BoundStatementBuilder statement)
    {
        if (!value.isJsonObject()) {
            throw new RuntimeException(value.toJson() + " is not map value");
        }
        GenericType genericType = ValueConverter.buildGenericTypeFromDataType(cassandraColumn.getType());
        statement.set(cassandraColumn.getName(), ValueConverter.convertValue(cassandraColumn.getType(), value), genericType);
    }
}

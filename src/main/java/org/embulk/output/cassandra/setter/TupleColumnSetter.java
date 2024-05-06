package org.embulk.output.cassandra.setter;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.TupleType;
import org.embulk.output.cassandra.converter.ValueConverter;
import org.embulk.spi.json.JsonValue;

import java.util.List;

public class TupleColumnSetter extends CassandraColumnSetter
{
    public TupleColumnSetter(ColumnMetadata cassandraColumn)
    {
        super(cassandraColumn);
    }

    @Override
    public void setJsonValue(JsonValue value, BoundStatementBuilder statement)
    {
        if (!value.isJsonArray()) {
            throw new RuntimeException(value.toJson() + " is not array value");
        }

        TupleType tupleType = (TupleType) cassandraColumn.getType();
        TupleValue tupleValue = ValueConverter.convertToTupleValue(tupleType, value);
        statement.setTupleValue(cassandraColumn.getName(), tupleValue);
    }
}

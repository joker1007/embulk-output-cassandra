package org.embulk.output.cassandra.setter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import org.msgpack.value.Value;

import java.util.List;

public class TupleColumnSetter extends CassandraColumnSetter
{
    private final Cluster cluster;

    public TupleColumnSetter(ColumnMetadata cassandraColumn, Cluster cluster)
    {
        super(cassandraColumn);
        this.cluster = cluster;
    }

    @Override
    public void setJsonValue(Value value, BoundStatement statement)
    {
        if (!value.isArrayValue()) {
            throw new RuntimeException(value.toJson() + " is not array value");
        }

        TupleType tupleType = cluster.getMetadata().newTupleType(((TupleType) cassandraColumn.getType()).getComponentTypes());
        List<Object> list = ValueConverter.convertList(value.asArrayValue().list());
        TupleValue tupleValue = tupleType.newValue(list.toArray());
        statement.setTupleValue(cassandraColumn.getName(), tupleValue);
    }
}

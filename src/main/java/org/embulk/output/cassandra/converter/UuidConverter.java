package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import org.embulk.spi.json.JsonValue;

import java.util.UUID;

public class UuidConverter implements Converter
{
    @Override
    public UUID convertJsonValue(DataType dataType, JsonValue value)
    {
        if (value.isJsonNull()) {
            return Uuids.random();
        } else {
            throw new UnsupportedOperationException(exceptionMessage(value));
        }
    }
}

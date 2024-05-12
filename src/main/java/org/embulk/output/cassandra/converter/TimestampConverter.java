package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

import java.time.Instant;
import java.time.LocalDate;

public class TimestampConverter implements Converter
{
    @Override
    public Instant convertJsonValue(DataType dataType, JsonValue value)
    {
        if (value.isJsonNull()) {
            return null;
        }

        if (value.isJsonString()) {
            return Instant.parse(value.asJsonString().getString());
        } else {
            throw new UnsupportedOperationException(exceptionMessage(value));
        }
    }
}

package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class SmallintConverter implements Converter
{
    @Override
    public Short convertJsonValue(DataType dataType, JsonValue value)
    {
        if (value.isJsonNull()) {
            return null;
        }

        if (value.isJsonBoolean()) {
            return (short) (value.asJsonBoolean().booleanValue() ? 1 : 0);
        } else if (value.isJsonDouble()) {
            return value.asJsonDouble().shortValue();
        } else if (value.isJsonString()) {
            return Short.parseShort(value.asJsonString().getString());
        } else if (value.isJsonLong()) {
            return value.asJsonLong().shortValue();
        } else {
            throw new UnsupportedOperationException(exceptionMessage(value));
        }
    }
}

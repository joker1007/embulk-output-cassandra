package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

import java.math.BigInteger;

public class VarintConverter implements Converter
{
    @Override
    public BigInteger convertJsonValue(DataType dataType, JsonValue value)
    {
        if (value.isJsonNull()) {
            return null;
        }

        if (value.isJsonBoolean()) {
            return value.asJsonBoolean().booleanValue() ? BigInteger.ONE : BigInteger.ZERO;
        } else if (value.isJsonDouble()) {
            return BigInteger.valueOf(value.asJsonDouble().longValue());
        } else if (value.isJsonString()) {
            return new BigInteger(value.asJsonString().getString());
        } else if (value.isJsonLong()) {
            return BigInteger.valueOf(value.asJsonLong().longValue());
        } else {
            throw new UnsupportedOperationException(exceptionMessage(value));
        }
    }
}

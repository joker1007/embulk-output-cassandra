package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class InetConverter implements Converter
{
    @Override
    public InetAddress convertJsonValue(DataType dataType, JsonValue value)
    {
        if (value.isJsonNull()) {
            return null;
        }

        if (value.isJsonString()) {
            try {
                return InetAddress.getByName(value.asJsonString().getString());
            } catch (UnknownHostException e) {
                throw new UnsupportedOperationException(e);
            }
        } else {
            throw new UnsupportedOperationException(exceptionMessage(value));
        }
    }
}

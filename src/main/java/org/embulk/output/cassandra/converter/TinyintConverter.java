package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class TinyintConverter implements Converter {
  @Override
  public Byte convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonBoolean()) {
      return (byte) (value.asJsonBoolean().booleanValue() ? 1 : 0);
    } else if (value.isJsonDouble()) {
      return value.asJsonDouble().byteValue();
    } else if (value.isJsonString()) {
      return Byte.parseByte(value.asJsonString().getString());
    } else if (value.isJsonLong()) {
      return value.asJsonLong().byteValue();
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

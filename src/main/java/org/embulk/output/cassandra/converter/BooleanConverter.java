package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class BooleanConverter implements Converter {
  @Override
  public Boolean convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonBoolean()) {
      return value.asJsonBoolean().booleanValue();
    } else if (value.isJsonDouble()) {
      return value.asJsonDouble().longValue() != 0;
    } else if (value.isJsonString()) {
      return Boolean.parseBoolean(value.asJsonString().getString());
    } else if (value.isJsonLong()) {
      return value.asJsonLong().longValue() != 0;
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

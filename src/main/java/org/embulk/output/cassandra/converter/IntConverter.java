package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class IntConverter implements Converter {
  @Override
  public Integer convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonBoolean()) {
      return value.asJsonBoolean().booleanValue() ? 1 : 0;
    } else if (value.isJsonDouble()) {
      return value.asJsonDouble().intValue();
    } else if (value.isJsonString()) {
      return Integer.parseInt(value.asJsonString().getString());
    } else if (value.isJsonLong()) {
      return value.asJsonLong().intValue();
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

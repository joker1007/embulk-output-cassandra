package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class BigintConverter implements Converter {
  @Override
  public Long convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonBoolean()) {
      return value.asJsonBoolean().booleanValue() ? 1L : 0L;
    } else if (value.isJsonDouble()) {
      return value.asJsonDouble().longValue();
    } else if (value.isJsonString()) {
      return Long.parseLong(value.asJsonString().getString());
    } else if (value.isJsonLong()) {
      return value.asJsonLong().longValue();
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

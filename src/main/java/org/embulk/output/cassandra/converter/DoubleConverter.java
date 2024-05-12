package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class DoubleConverter implements Converter {
  @Override
  public Double convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonBoolean()) {
      return value.asJsonBoolean().booleanValue() ? 1.0d : 0.0d;
    } else if (value.isJsonDouble()) {
      return value.asJsonDouble().doubleValue();
    } else if (value.isJsonString()) {
      return Double.parseDouble(value.asJsonString().getString());
    } else if (value.isJsonLong()) {
      return value.asJsonLong().doubleValue();
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

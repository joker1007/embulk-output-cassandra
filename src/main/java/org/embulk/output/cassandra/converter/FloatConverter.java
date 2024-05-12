package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class FloatConverter implements Converter {
  @Override
  public Float convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonBoolean()) {
      return value.asJsonBoolean().booleanValue() ? 1.0f : 0.0f;
    } else if (value.isJsonDouble()) {
      return value.asJsonDouble().floatValue();
    } else if (value.isJsonString()) {
      return Float.parseFloat(value.asJsonString().getString());
    } else if (value.isJsonLong()) {
      return value.asJsonLong().floatValue();
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

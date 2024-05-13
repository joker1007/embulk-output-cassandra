package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import java.time.LocalTime;
import org.embulk.spi.json.JsonValue;

public class TimeConverter implements Converter {
  @Override
  public LocalTime convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonString()) {
      return LocalTime.parse(value.asJsonString().getString());
    } else if (value.isJsonLong()) {
      return LocalTime.ofNanoOfDay(value.asJsonLong().longValue());
    } else if (value.isJsonDouble()) {
      return LocalTime.ofNanoOfDay(value.asJsonDouble().longValue());
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

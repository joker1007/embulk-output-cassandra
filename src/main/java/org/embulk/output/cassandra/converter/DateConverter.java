package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import java.time.LocalDate;
import org.embulk.spi.json.JsonValue;

public class DateConverter implements Converter {
  @Override
  public LocalDate convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonString()) {
      return LocalDate.parse(value.asJsonString().getString());
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

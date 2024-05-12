package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class DurationConverter implements Converter {
  @Override
  public CqlDuration convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonString()) {
      return CqlDuration.from(value.asJsonString().getString());
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

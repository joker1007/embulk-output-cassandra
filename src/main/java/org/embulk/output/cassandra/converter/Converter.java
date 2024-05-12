package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public interface Converter {
  Object convertJsonValue(DataType dataType, JsonValue value);

  default String exceptionMessage(JsonValue value) {
    return "Unsupported type: " + value.getEntityType() + " for " + this.getClass().getName();
  }
}

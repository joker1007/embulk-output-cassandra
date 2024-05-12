package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import org.embulk.spi.json.JsonValue;

public class StringConverter implements Converter {
  @Override
  public String convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (!value.isJsonString()) {
      return value.toJson();
    } else {
      return value.asJsonString().getString();
    }
  }
}

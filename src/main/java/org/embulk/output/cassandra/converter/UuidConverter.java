package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.util.UUID;
import org.embulk.spi.json.JsonValue;

public class UuidConverter implements Converter {
  @Override
  public UUID convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return Uuids.random();
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

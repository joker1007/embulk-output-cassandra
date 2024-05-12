package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.type.DataType;
import java.math.BigDecimal;
import org.embulk.spi.json.JsonValue;

public class DecimalConverter implements Converter {
  @Override
  public BigDecimal convertJsonValue(DataType dataType, JsonValue value) {
    if (value.isJsonNull()) {
      return null;
    }

    if (value.isJsonBoolean()) {
      return value.asJsonBoolean().booleanValue() ? BigDecimal.valueOf(1L) : BigDecimal.valueOf(0L);
    } else if (value.isJsonDouble()) {
      return BigDecimal.valueOf(value.asJsonDouble().doubleValue());
    } else if (value.isJsonString()) {
      return new BigDecimal(value.asJsonString().getString());
    } else if (value.isJsonLong()) {
      return BigDecimal.valueOf(value.asJsonLong().longValue());
    } else {
      throw new UnsupportedOperationException(exceptionMessage(value));
    }
  }
}

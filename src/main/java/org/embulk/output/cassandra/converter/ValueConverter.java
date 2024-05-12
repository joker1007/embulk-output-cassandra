package org.embulk.output.cassandra.converter;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.embulk.spi.json.JsonArray;
import org.embulk.spi.json.JsonNull;
import org.embulk.spi.json.JsonString;
import org.embulk.spi.json.JsonValue;

public class ValueConverter {
  private ValueConverter() {}

  public static GenericType<?> buildGenericTypeFromDataType(DataType dataType) {
    if (dataType instanceof PrimitiveType) {
      if (dataType.equals(DataTypes.ASCII)) {
        return GenericType.STRING;
      } else if (dataType.equals(DataTypes.BIGINT)) {
        return GenericType.LONG;
      } else if (dataType.equals(DataTypes.BLOB)) {
        return GenericType.arrayOf(byte.class);
      } else if (dataType.equals(DataTypes.BOOLEAN)) {
        return GenericType.BOOLEAN;
      } else if (dataType.equals(DataTypes.COUNTER)) {
        return GenericType.LONG;
      } else if (dataType.equals(DataTypes.DECIMAL)) {
        return GenericType.BIG_DECIMAL;
      } else if (dataType.equals(DataTypes.DOUBLE)) {
        return GenericType.DOUBLE;
      } else if (dataType.equals(DataTypes.DURATION)) {
        return GenericType.CQL_DURATION;
      } else if (dataType.equals(DataTypes.FLOAT)) {
        return GenericType.FLOAT;
      } else if (dataType.equals(DataTypes.INET)) {
        return GenericType.INET_ADDRESS;
      } else if (dataType.equals(DataTypes.INT)) {
        return GenericType.INTEGER;
      } else if (dataType.equals(DataTypes.SMALLINT)) {
        return GenericType.SHORT;
      } else if (dataType.equals(DataTypes.TEXT)) {
        return GenericType.STRING;
      } else if (dataType.equals(DataTypes.TINYINT)) {
        return GenericType.BYTE;
      } else if (dataType.equals(DataTypes.TIMESTAMP)) {
        return GenericType.INSTANT;
      } else if (dataType.equals(DataTypes.TIMEUUID)) {
        return GenericType.UUID;
      } else if (dataType.equals(DataTypes.UUID)) {
        return GenericType.UUID;
      } else if (dataType.equals(DataTypes.VARINT)) {
        return GenericType.BIG_INTEGER;
      } else {
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
      }
    } else if (dataType instanceof ListType) {
      ListType listType = (ListType) dataType;
      return GenericType.listOf(buildGenericTypeFromDataType(listType.getElementType()));
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      return GenericType.mapOf(
          buildGenericTypeFromDataType(mapType.getKeyType()),
          buildGenericTypeFromDataType(mapType.getValueType()));
    } else if (dataType instanceof SetType) {
      SetType setType = (SetType) dataType;
      return GenericType.setOf(buildGenericTypeFromDataType(setType.getElementType()));
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  public static TupleValue convertToTupleValue(TupleType tupleType, JsonValue value) {
    if (!value.isJsonArray()) {
      throw new UnsupportedOperationException(value.toJson() + " is not array value");
    }
    List<DataType> componentTypes = tupleType.getComponentTypes();
    JsonArray jsonArray = value.asJsonArray();
    Object[] result = new Object[componentTypes.size()];
    for (int i = 0; i < componentTypes.size(); i++) {
      if (i >= jsonArray.size()) {
        result[i] = convertValue(componentTypes.get(i), JsonNull.of());
      }
      result[i] = convertValue(componentTypes.get(i), jsonArray.get(i));
    }
    return tupleType.newValue(result);
  }

  public static Object convertValue(DataType dataType, JsonValue value) {
    if (!dataType.equals(DataTypes.UUID)
        && !dataType.equals(DataTypes.TIMEUUID)
        && value.isJsonNull()) {
      return null;
    }

    if (dataType instanceof ListType) {
      if (!value.isJsonArray()) {
        throw new UnsupportedOperationException(value.toJson() + " is not array value");
      }
      ListType listType = (ListType) dataType;
      List<Object> result = Lists.newArrayList();
      value.asJsonArray().forEach(v -> result.add(convertValue(listType.getElementType(), v)));
      return result;
    } else if (dataType instanceof MapType) {
      if (!value.isJsonObject()) {
        throw new UnsupportedOperationException(value.toJson() + " is not object value");
      }
      MapType mapType = (MapType) dataType;
      Map<Object, Object> result = new HashMap<>();
      value
          .asJsonObject()
          .forEach(
              (k, v) ->
                  result.put(
                      convertValue(mapType.getKeyType(), JsonString.of(k)),
                      convertValue(mapType.getValueType(), v)));
      return result;
    } else if (dataType instanceof SetType) {
      if (!value.isJsonArray()) {
        throw new UnsupportedOperationException(value.toJson() + " is not array value");
      }
      SetType setType = (SetType) dataType;
      Set<Object> result = new HashSet<>();
      value.asJsonArray().forEach(v -> result.add(convertValue(setType.getElementType(), v)));
      return result;
    } else if (dataType instanceof TupleType) {
      TupleType tupleType = (TupleType) dataType;
      return convertToTupleValue(tupleType, value);
    } else if (dataType.equals(DataTypes.ASCII)) {
      return Converters.STRING.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.BIGINT)) {
      return Converters.BIGINT.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.BOOLEAN)) {
      return Converters.BOOLEAN.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.COUNTER)) {
      return Converters.BIGINT.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.DECIMAL)) {
      return Converters.DECIMAL.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.DOUBLE)) {
      return Converters.DOUBLE.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.DURATION)) {
      return Converters.DURATION.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.FLOAT)) {
      return Converters.FLOAT.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.INET)) {
      return Converters.INET.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.INT)) {
      return Converters.INT.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.SMALLINT)) {
      return Converters.SMALLINT.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.TEXT)) {
      return Converters.STRING.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.TINYINT)) {
      return Converters.TINYINT.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.TIMESTAMP)) {
      return Converters.TIMESTAMP.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.TIMEUUID)) {
      return Converters.TIMEUUID.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.UUID)) {
      return Converters.UUID.convertJsonValue(dataType, value);
    } else if (dataType.equals(DataTypes.VARINT)) {
      return Converters.VARINT.convertJsonValue(dataType, value);
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }
}

package org.embulk.output.cassandra.setter;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.msgpack.value.Value;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ValueConverter {
    public static List<Object> convertList(List<Value> list)
    {
        return Lists.transform(list, (val) -> convertValueToPlain(val));
    }

    public static Map<Object, Object> convertMap(Map<Value, Value> map)
    {
        return map.entrySet().stream().collect(Collectors.toMap(
                (Map.Entry<Value, Value> entry) -> {
                    if (!entry.getKey().isStringValue())
                        throw new RuntimeException("map key is");
                    return entry.getKey().asStringValue().toString();
                },
                (Map.Entry<Value, Value> entry) -> {
                    Value val = entry.getValue();
                    return convertValueToPlain(val);
                }
        ));
    }

    private static Object convertValueToPlain(Value val)
    {
        if (val.isNilValue())
            return null;
        if (val.isStringValue())
            return val.asStringValue().toString();
        if (val.isBooleanValue())
            return val.asBooleanValue().getBoolean();
        if (val.isIntegerValue())
            return val.asIntegerValue().asLong();
        if (val.isFloatValue())
            return val.asFloatValue().toDouble();
        if (val.isArrayValue())
            return convertList(val.asArrayValue().list());
        if (val.isMapValue())
            return convertMap(val.asMapValue().map());

        return null;
    }
}

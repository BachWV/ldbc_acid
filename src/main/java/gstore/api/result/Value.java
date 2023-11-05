package gstore.api.result;

import com.fasterxml.jackson.databind.JsonNode;
import gstore.util.JsonUtil;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Map;

public class Value {

    private String type;

    private String datatype;

    private Object value;

    public Value() { }

    public Value(String type, String datatype, Object value) {
        this.type = type;
        this.datatype = datatype;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String asString() {
        return String.valueOf(value);
    }

    public long asLong() {
        if ("http://www.w3.org/2001/XMLSchema#long".equalsIgnoreCase(datatype) ||
                "http://www.w3.org/2001/XMLSchema#int".equalsIgnoreCase(datatype) ||
                "http://www.w3.org/2001/XMLSchema#integer".equalsIgnoreCase(datatype)) {
            try {
                if (value != null && !"".equals(value)) {
                    return Long.parseLong(String.valueOf(value));
                } else {
                    throw new NumberFormatException("value is blank");
                }
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Cannot coerce String to long without losing precision");
            }
        } else {
            throw new RuntimeException("Defined data type is " + datatype);
        }
    }

    public int asInt() {
        if ("http://www.w3.org/2001/XMLSchema#int".equalsIgnoreCase(datatype) ||
                "http://www.w3.org/2001/XMLSchema#integer".equalsIgnoreCase(datatype)) {
            try {
                if (value != null && !"".equals(value)) {
                    return Integer.parseInt(asString());
                } else {
                    throw new NumberFormatException("value is blank");
                }
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Cannot coerce String to int without losing precision");
            }
        } else {
            throw new RuntimeException("Defined data type is " + datatype);
        }
    }

    public double asDouble() {
        if ("http://www.w3.org/2001/XMLSchema#double".equalsIgnoreCase(datatype)) {
            try {
                if (value != null && !"".equals(value)) {
                    return Double.parseDouble(asString());
                } else {
                    throw new NumberFormatException("value is blank");
                }
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Cannot coerce String to double without losing precision");
            }
        } else {
            throw new RuntimeException("Defined data type is " + datatype);
        }
    }

    public long asDateTimeLong() {
        if ("http://www.w3.org/2001/XMLSchema#dateTime".equalsIgnoreCase(datatype)) {
            try {
                Instant instant = Instant.parse(asString());
                return instant.toEpochMilli();
            } catch (DateTimeParseException e) {
                throw new RuntimeException("Cannot coerce String to timestamp long without losing precision");
            }
        } else {
            throw new RuntimeException("Defined data type is " + datatype);
        }
    }

    public long asDateLong() {
        if ("http://www.w3.org/2001/XMLSchema#date".equalsIgnoreCase(datatype)) {
            try {
                return LocalDate.parse(asString()).atStartOfDay(ZoneId.of("GMT")).toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                throw new RuntimeException("Cannot coerce String to date long without losing precision");
            }
        } else {
            throw new RuntimeException("Defined data type is " + datatype);
        }
    }

    public boolean asBoolean() {
        if ("http://www.w3.org/2001/XMLSchema#boolean".equalsIgnoreCase(datatype)) {
            return Boolean.parseBoolean(asString());
        } else if ("true".equals(value) || "false".equals(value)) {
            return Boolean.parseBoolean(asString());
        } else {
            throw new RuntimeException("Defined data type is " + datatype);
        }
    }

    public JsonNode asJSON() {
        if (value instanceof String){
            return JsonUtil.toJson(asString());
        } else if (value instanceof Map) {
            String str = JsonUtil.toJson(value);
            return JsonUtil.toJson(str);
        } else {
            return null;
        }
    }
}

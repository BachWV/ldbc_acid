package gstore.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;

public class JsonUtil {

    private static final SimpleDateFormat df_yyyymmddhhmmss = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final SimpleDateFormat df_yyyymmdd = new SimpleDateFormat("yyyy-MM-dd");

    public static ObjectMapper o2m = new ObjectMapper();

    static {
        o2m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String toJson(Object obj) {
        if (obj == null) {
            return null;
        }
        String jsonStr = null;
        try {
            jsonStr = o2m.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonStr;
    }

    public static JsonNode toJson(String json) {
        if (json == null || "".equals(json)) {
            return null;
        }
        JsonNode jsonNode = null;
        try {
            jsonNode = o2m.readTree(json);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonNode;
    }



    public static <T> T toBean(JsonNode json, Class<T> clazz) {
        if (json == null || clazz == null) {
            return null;
        }
        try {
            return o2m.treeToValue(json, clazz);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

}

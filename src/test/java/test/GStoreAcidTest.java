package test;

import gstore.GStoreDriver;

import java.util.HashMap;
import java.util.Map;
public class GStoreAcidTest extends AcidTest<GStoreDriver>{
    private static final String endpoint = "http://xxxxx:9000";
    private static final String username = "root";
    private static final String password = "123456";
    private static final String dbName = "lubm";
    private static final String accessMode = "rpc";
    private static final String isoLevel = "3"; // serialization

    public static Map<String, String> getProperties() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("endpoint", endpoint);
        properties.put("username", username);
        properties.put("password", password);
        properties.put("dbName", dbName);
        properties.put("accessMode", accessMode);
        properties.put("isolevel", isoLevel);
        properties.put("printQueryNames", "true");
        properties.put("printQueryStrings", "true");
        properties.put("printQueryResults", "true");
        return properties;
    }

    public GStoreAcidTest(){
        super(new GStoreDriver(getProperties()));
    }

}

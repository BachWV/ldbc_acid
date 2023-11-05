package gstore.api.http;

public enum AccessMode {

    HTTP(""),

    RPC("grpc/api");

    public String suffix;

    private AccessMode(String suffix) {
        this.suffix = suffix;
    }

    public static AccessMode getAccessMode(String name) {
        if (RPC.name().equalsIgnoreCase(name)) {
            return RPC;
        } else {
            return HTTP;
        }
    }
}

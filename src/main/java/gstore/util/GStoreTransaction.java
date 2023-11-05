package gstore.util;
import gstore.api.http.GStoreConnector;
import gstore.api.result.Result;

public class GStoreTransaction {
    String tid;
    GStoreConnector conn;

    public GStoreTransaction(String tid, GStoreConnector connector) {
        this.tid=tid;
        this.conn=connector;
    }

    public GStoreConnector getConn() {
        return conn;
    }

    public String getTid() {
        return tid;
    }
    public Result execute(String query) throws Exception {
        return conn.execute(tid,query);
    }

    public Result rollback() throws Exception {
        return conn.rollback(tid);
    }
    public Result commit() throws Exception {
        return conn.rollback(tid);
    }
}

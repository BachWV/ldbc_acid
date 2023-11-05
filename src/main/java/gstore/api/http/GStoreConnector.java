package gstore.api.http;

import gstore.api.result.Result;
import gstore.util.JsonUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

public class GStoreConnector {

    private String uri;

    private String username;

    private String password;

    private String dbName;

    private AccessMode accessMode;

    private int timeout;

    private GStoreConnector(String uri, String username, String password, String dbName, AccessMode accessMode, int timeout) {
        this.uri = uri;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        this.accessMode = accessMode;
        this.timeout = timeout;
    }

    public static GStoreConnector getInstance(String uri, String username, String password, String dbName, AccessMode accessMode, int timeout) {
        String real_uri = null;
        if (!uri.endsWith("/")) {
            real_uri = uri + "/";
        } else {
            real_uri = uri;
        }
        real_uri = real_uri + accessMode.suffix;
        return new GStoreConnector(real_uri, username, password, dbName, accessMode, timeout);
    }

    public static GStoreConnector getInstance(String uri, String username, String password, String dbName, AccessMode accessMode) {
        return getInstance(uri, username, password, dbName, accessMode, 5 * 60 * 1000);
    }

    public static GStoreConnector getInstance(String uri, String username, String password, String dbName, int timeout) {
        return getInstance(uri, username, password, dbName, AccessMode.RPC, timeout);
    }

    public static GStoreConnector getInstance(String uri, String username, String password, String dbName) {
        return getInstance(uri, username, password, dbName, AccessMode.RPC);
    }

    public Result run(String sparql) throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("operation", "query");
        map.put("sparql", sparql);
        String result_str = sendPost(this.uri, map, timeout);
        return Result.instance().build(result_str);
    }

    public Result execute(String tid, String sparql) throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("operation", "tquery");
        map.put("tid", tid);
        map.put("sparql", sparql);
        String result_str = sendPost(this.uri, map, timeout);
        return Result.instance().build(result_str);
    }

    public Result begin(String level) throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("operation", "begin");
        map.put("isolevel", level);
        String result_str = sendPost(this.uri, map, timeout);
        return Result.instance().build(result_str);
    }

    public Result commit(String tid) throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("operation", "commit");
        map.put("tid", tid);
        String result_str = sendPost(this.uri,  map, timeout);
        return Result.instance().build(result_str);
    }

    public Result rollback(String tid) throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("operation", "rollback");
        map.put("tid", tid);
        String result_str = sendPost(this.uri,  map, timeout);
        return Result.instance().build(result_str);
    }

    public Result checkpoint() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("operation", "checkpoint");
        String result_str = sendPost(this.uri,  map, timeout);
        return Result.instance().build(result_str);
    }

    public boolean testConnect() {
        Map<String, String> map = new HashMap<>();
        map.put("operation", "login");
        String result_str = sendPost(this.uri, map, timeout);
        Result rt = null;
        try {
            rt = Result.instance().build(result_str);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (rt != null) {
            return rt.success();
        } else {
            return false;
        }
    }


    private String sendPost(String url, Map<String, String> paramsMap, int timeout) {
        PrintWriter out = null;
        StringBuffer result = new StringBuffer();
        BufferedReader in = null;
        String returnResult = null;
        paramsMap.put("username", this.username);
        paramsMap.put("password", this.password);
        paramsMap.put("db_name", this.dbName);
        if (!paramsMap.containsKey("format")) {
            paramsMap.put("format", "json");
        }
        String strPost = JsonUtil.toJson(paramsMap);
        long t1 = System.currentTimeMillis(); // ms
        try {
            URL realUrl = new URL(url);
            URLConnection connection = realUrl.openConnection();
            if(accessMode.equals(AccessMode.RPC)) {
                connection.setRequestProperty("Content-Type", "application/json;");
            } else {
                connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;");
            }
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.setConnectTimeout(60*1000); // default 60s
            connection.setReadTimeout(timeout);
            connection.setDoOutput(true);
            connection.setDoInput(true);
            out = new PrintWriter(connection.getOutputStream());
            out.print(strPost);
            out.flush();

            // define BufferedReader to read the response of the URL
            in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
            String line;
            while ((line = in.readLine()) != null) {
                result.append(line + "\n");
            }
        } catch (Exception e) {
            System.err.println("error in post request:" + e);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            long t2 = System.currentTimeMillis(); //ms
            returnResult = result.toString();
            long threadId = Thread.currentThread().getId();
            System.out.println("\n######## thread-" + threadId + "########"
                    + "\nurl:" + url
                    + "\n参数：" + strPost
                    + "\n结果：" + (result.length() > 500 ? (result.substring(0, 500) + "......") : result)
                    + "\n耗时(ms)：" + (t2 - t1));
            if(returnResult == null || "".equals(returnResult)) {
                throw new RuntimeException("access timeout");
            }
        }
        return returnResult;
    }
}

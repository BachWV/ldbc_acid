package gstore;
import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import gstore.api.http.AccessMode;
import gstore.api.http.GStoreConnector;
import gstore.api.result.Record;
import gstore.api.result.Result;
import gstore.util.GStoreTransaction;
import org.eclipse.rdf4j.query.BindingSet;
import org.junit.Assert;

import java.util.*;

public class GStoreDriver  extends TestDriver<GStoreTransaction, Map<String, Object>, BindingSet>{
    GStoreConnector connector;
    String isoLevel;
    public GStoreDriver(Map<String, String> properties) {
        try {
            final String endpointURI = properties.get("endpoint");
            final String username = properties.get("username");
            final String password = properties.get("password");
            final String dbName = properties.get("dbName");
            final String accessMode = properties.get("accessMode");
            isoLevel = properties.get("isolevel");
            connector = GStoreConnector.getInstance(endpointURI, username, password, dbName, AccessMode.getAccessMode(accessMode), 600000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void atomicityInit() {
        String ato_init = "INSERT DATA {\n" +
                "    <pers1>\n" +
                "        <type> <Person> ;\n" +
                "        <id> \"1\" ;\n" +
                "        <name> \"Alice\" ;\n" +
                "        <email> \"alice@aol.com\" .\n" +
                "    <pers2>\n" +
                "        <type> <Person> ;\n" +
                "        <id> \"2\" ;\n" +
                "        <name> \"Bob\" ;\n" +
                "        <email> \"bob@hotmail.com\" ;\n" +
                "        <email> \"bobby@yahoo.com\" .\n" +
                "}";
        executeUpdate_checkpoint(ato_init);
    }
@Override
    public void atomicityC(Map<String, Object> parameters) {
        String ato_c = "insert data {\n" +
                "    <pers1> <knows> <_knows> .\n" +
                "    <_knows> <hasPerson> <pers3> .\n" +
                "    <pers3> <type> <Person> .\n" +
                "    <_knows> <creationDate> \"2020\" .\n" +
                "    <pers3> <id> \"3\" .\n" +
                "    <pers1> <email> \"alice@otherdomain.net\" .\n" +
                "}";
        executeUpdate_checkpoint(ato_c);
    }
@Override
    public void atomicityRB(Map<String, Object> parameters) {
        try {
            GStoreTransaction conn = startTransaction();
            String preparequery = "INSERT DATA {\n" +
                    "\t<pers1> <email> \"alice@otherdomain.net\" . \n" +
                    "}";

            conn.execute(preparequery);
            String rb2 = "select ?p where { \n" +
                    "    ?p <id> \"2\" .\n" +
                    "}";
            Result queryResult = conn.execute(rb2);
            System.out.println("atomicityRB result");
            if (queryResult.records().size() > 0) {
                abortTransaction(conn);
            } else {
                String rb3 = "INSERT DATA {\n" +
                        "    <pers3> <type> <Person> .\n" +
                        "    <pers3> <id> \"3\" .\n" +
                        "}";
                conn.execute( rb3);
                commitTransaction(conn);
                // conn.checkpoint();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> atomicityCheck() {
        try {
            GStoreTransaction conn = startTransaction();
            // conn.execute(tid,"select ?x ?y ?z where {?x ?y ?z};");
            String query ="select (count (distinct ?p) as ?numPersons) (count (distinct ?name) as ?numNames) (count (distinct ?emails) as ?numEmails) where {" +
                    " ?p <type> <Person> ." +
                    "optional {?p <name> ?name .}" +
                    "optional {?p <email> ?emails .}}";
            Result result = conn.execute( query);
            if (result.records().size() == 0) {
                throw new IllegalStateException("AtomicityCheck missing from query result.");
            }
            Record r = result.records().get(0);
            long numPersons = r.values().get(r.index("numPersons")).asLong();
            long numNames = r.values().get(r.index("numNames")).asLong();
            long numEmails = r.values().get(r.index("numEmails")).asLong();
            commitTransaction(conn);
            //  conn.checkpoint();
            return ImmutableMap.of("numPersons", numPersons, "numNames", numNames, "numEmails", numEmails);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void g0Init() {
        String g0init = "insert data { \n" +
                "\t<pers1> <knows> <_knows> .\n" +
                "<_knows> <hasPerson> <pers2> .\n" +
                "<pers1> <versionHistory> 0 .\n" +
                "<pers2> <versionHistory> 0 .\n" +
                "<_knows> <versionHistory> 0 .\n" +
                "}";
        executeUpdate(g0init);
    }
    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {

        GStoreTransaction conn=startTransaction();

        try {
            executeUpdate( substituteParameters("insert { " +
                    "<pers%person1Id%> <versionHistory> %transactionId% .\n" +
                    "    <pers%person2Id%> <versionHistory> %transactionId% .\n" +
                    "    ?knows <versionHistory> %transactionId% .  \n" +
                    "} where {\n" +
                    "    <pers%person1Id%> <knows> ?knows .\n" +
                    "    ?knows <hasPerson> <pers%person2Id%> .\n" +
                    "}", parameters));
            commitTransaction(conn);;
            // conn.checkpoint();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        Assert.assertEquals(1,0); //skip the test
        final List<Object> p1VersionHistory = new ArrayList<>();
        final List<Object> kVersionHistory = new ArrayList<>();
        final List<Object> p2VersionHistory = new ArrayList<>();
        try {
            GStoreTransaction conn = startTransaction();
            String g0ch_q = "select ?p1VersionHistory where {\n" +
                    "    <pers%person1Id%> <versionHistory> ?p1VersionHistory .\n" +
                    "}";
            Result result = conn.execute( substituteParameters(g0ch_q, parameters));
            if (result.records().size() > 0) {
                for (Record rc : result.records()) {
                    Long l = rc.get(rc.index("p1VersionHistory")).asLong();
                    System.out.println(l);
                    p1VersionHistory.add(l);
                }
            }
            String g0ch_2 = "select ?p2VersionHistory where {\n" +
                    "    <pers%person2Id%> <versionHistory> ?p2VersionHistory .\n" +
                    "}";
            Result result2 = conn.execute(substituteParameters(g0ch_2, parameters));
            if (result2.records().size() > 0) {
                for (Record rc : result2.records()) {
                    Long l = rc.get(rc.index("p2VersionHistory")).asLong();
                    System.out.println(l);
                    p2VersionHistory.add(l);
                }
            }
            String g0ch_3 = "select ?kVersionHistory where {\n" +
                    "    <pers%person1Id%> <knows> <_knows> .\n" +
                    "    <_knows> <hasPerson> <pers%person2Id%> .\n" +
                    "    <_knows> <versionHistory> ?kVersionHistory .\n" +
                    "}";
            Result result3 = conn.execute(substituteParameters(g0ch_3, parameters));
            if (result3.records().size() > 0) {
                for (Record rc : result3.records()) {
                    Long l = rc.get(rc.index("kVersionHistory")).asLong();
                    System.out.println(l);
                    kVersionHistory.add(l);
                }
            }
            commitTransaction(conn);;
            //conn.checkpoint();
            return ImmutableMap.of("p1VersionHistory", p1VersionHistory, "p2VersionHistory", p2VersionHistory, "kVersionHistory", kVersionHistory);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void g1aInit() {
        executeUpdate("insert data {\n" +
                "     <pers1> <type> <Person> ;\n" +
                "     <id> 1 ;\n" +
                "     <version> 1 .\n" +
                "}");
    }
    @Override
    public Map<String, Object> g1aW(Map<String, Object> parameters) {
        GStoreTransaction conn = startTransaction();
        long personId;

        try {
            Result result = conn.execute( substituteParameters("select ?id where { \n" +
                    "<pers%personId%> <id> ?id .\n" +
                    "}", parameters));
            if (result.records().size() == 0) {
                throw new IllegalStateException("G1a TW missing person from query result.");
            }
            Record rc = result.records().get(0);
            System.out.println(rc.get(rc.index("id")));
            personId = rc.get(rc.index("id")).asLong();
            commitTransaction(conn);;
            //conn.checkpoint();
            sleep((Long) parameters.get("sleepTime"));
            conn.execute(substituteParameters(
                    "delete {\n" +
                            "  <pers%personId%> <version> ?v .\n" +
                            "} insert {\n" +
                            "    <pers%personId%> <version> 2 .\n" +
                            "} where {\n" +
                            "    <pers%personId%> <version> ?v .\n" +
                            "}", ImmutableMap.of("personId", personId)));
            sleep((Long) parameters.get("sleepTime"));
            abortTransaction(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ImmutableMap.of();
    }
    @Override
    public Map<String, Object> g1aR(Map<String, Object> parameters) {
        GStoreTransaction conn = startTransaction();
        try {
            Result result = conn.execute( substituteParameters(
                    "select ?pVersion where { \n" +
                            "\t<pers%personId%> <version> ?pVersion .\n" +
                            "}", parameters));
            if (result.records().size() == 0) {
                throw new IllegalStateException("G1A2 missing person from query result.");
            }
            Record rc = result.records().get(0);

            final long pVersion = rc.get(rc.index("pVersion")).asLong();
            //final long pVersion = Long.parseLong(queryResult.next().getValue("pVersion").stringValue());
            commitTransaction(conn);;
            //conn.checkpoint();
            return ImmutableMap.of("pVersion", pVersion);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void g1bInit() {
        executeUpdate("insert data {\n" +
                "<pers1> <type> <Person> ;\n" +
                "<id> 1  ;\n" +
                "<version> 99  .\n" +
                "}");
    }

    @Override
    public Map<String, Object> g1bW(Map<String, Object> parameters) {
        GStoreTransaction conn = startTransaction();
        try {
            conn.execute(substituteParameters("delete {\n" +
                    "     <pers%personId%> <version> ?p .\n" +
                    "} insert {\n" +
                    "     <pers%personId%> <version> %even%  .\n" +
                    "} where {\n" +
                    "     <pers%personId%> <version> ?p .\n" +
                    "}", parameters));
            sleep((Long) parameters.get("sleepTime"));
            conn.execute( substituteParameters("delete {\n" +
                    "     <pers%personId%> <version> ?p .\n" +
                    "} insert {\n" +
                    "     <pers%personId%> <version> %odd%  .\n" +
                    "} where {\n" +
                    "     <pers%personId%> <version> ?p .\n" +
                    "}", parameters));
            commitTransaction(conn);;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1bR(Map<String, Object> parameters) {
        GStoreTransaction conn = startTransaction();
        try {
            Result result = conn.execute(substituteParameters("select ?pVersion where { \n" +
                    "\t <pers%personId%> <version> ?pVersion .\n" +
                    "}", parameters));
            if (result.records().size() == 0) {
                throw new IllegalStateException("G1B missing person from query result.");
            }
            Record rc = result.single();

            final long pVersion = rc.get(rc.index("pVersion")).asLong();
            commitTransaction(conn);;
            return ImmutableMap.of("pVersion", pVersion);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void g1cInit() {
        executeUpdate("INSERT DATA {\n" +
                "<pers1> <type> <Person> ;\n" +
                "        <id> 1  ;\n" +
                "\t\t<version> 0  . \n" +
                "<pers2> <type> <Person> ;\n" +
                "        <id> 2  ;\n" +
                "\t\t<version> 0  .\n" +
                "}");
    }

    @Override
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        long pVersion;
        try {
            GStoreTransaction conn = startTransaction();
            conn.execute( substituteParameters(
                    "delete {\n" +
                            "     <pers%person1Id%> <version> ?p .\n" +
                            "} insert {\n" +
                            "     <pers%person1Id%> <version> %transactionId% .\n" +
                            "} where {\n" +
                            "     <pers%person1Id%> <version> ?p .\n" +
                            "}", parameters));

            Result result = conn.execute( substituteParameters("select ?person2Version where { \n" +
                    "\t <pers%person2Id%> <version> ?person2Version .\n" +
                    "}", parameters));
            if (result.records().size() == 0) {
                throw new IllegalStateException("G1C missing person from query result.");
            }
            Record rc = result.single();
            pVersion = rc.get(rc.index("person2Version")).asLong();
            commitTransaction(conn);;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of("person2Version", pVersion);
    }

    @Override
    public void impInit() {
        executeUpdate("insert data {\n" +
                "     pers1 <type> <Person> ;\n" +
                "             <id> \"1\"  ;\n" +
                "             <version> \"1\"  .\n" +
                "}");
    }

    @Override
    public Map<String, Object> impW(Map<String, Object> parameters) {
        executeUpdate(substituteParameters("delete {\n" +
                "     <pers%personId%> <version> ?v .\n" +
                "} insert {\n" +
                "     <pers%personId%> <version> ?newV .\n" +
                "} where {\n" +
                "     <pers%personId%> <version> ?v .\n" +
                "    bind(?v + 1 as ?newV) .\n" +
                "}", parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> impR(Map<String, Object> parameters) {
        long firstRead = 0;
        long secondRead = 0;
        GStoreTransaction conn = startTransaction();

        try {
            Result result = conn.execute( substituteParameters(
                    "select ?firstRead where {\n" +
                            "      <pers%personId%> <version> ?firstRead .\n" +
                            "}", parameters));
            if(result.records().size()==0) {
                throw new IllegalStateException("IMP missing query result.");
            }
            Record rc=result.records().get(0);
            firstRead =rc.get(rc.index("firstRead")).asLong();

            sleep((Long) parameters.get("sleepTime"));
            Result result2 = conn.execute(substituteParameters(
                    "select ?secondRead where {\n" +
                            "      <pers%personId%> <version> ?secondRead .\n" +
                            "}", parameters));
            if(result2.records().size()==0) {
                throw new IllegalStateException("IMP missing query result.");
            }
            Record rc2=result2.records().get(0);
            secondRead =rc2.get(rc2.index("secondRead")).asLong();
            commitTransaction(conn);;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }
    public void pmpInit() {
        executeUpdate("INSERT DATA {\n" +
                "     <pers1> <type> <Person> ;\n" +
                "        <id> \"1\"  .\n" +
                "     <post1> <type> <Post> ;\n" +
                "        <id> \"1\"  .\n" +
                "}");
    }
    @Override
    public Map<String, Object> pmpW(Map<String, Object> parameters) {
        executeUpdate(substituteParameters(
                "insert data {\n" +
                        "      <pers%personId%> <likes>  <post%postId%> .\n" +
                        "} ", parameters));
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> pmpR(Map<String, Object> parameters) {
        long firstRead;
        long secondRead;
        GStoreTransaction conn = startTransaction();
        try {
            Result result = conn.execute(substituteParameters(
                    "select (count( <pers%personId%>) as ?firstRead) where {\n" +
                            "     <pers%personId%> <likes>  post%postId% .\n" +
                            "}", parameters));
            if(result.records().size()==0) {
                throw new IllegalStateException("PMP missing query result.");
            }
            Record rc=result.records().get(0);
            firstRead =rc.get(rc.index("firstRead")).asLong();

            sleep((Long) parameters.get("sleepTime"));
            Result result2 = conn.execute( substituteParameters(
                    "select (count( <pers%personId%>) as ?secondRead) where {\n" +
                            "     <pers%personId%> <likes>  post%postId% .\n" +
                            "}", parameters));
            if(result2.records().size()==0) {
                throw new IllegalStateException("PMP missing query result.");
            }
            Record rc2=result2.records().get(0);
            secondRead =rc2.get(rc2.index("secondRead")).asLong();


            commitTransaction(conn);;
        }  catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }
    @Override
    public void otvInit() {
        initData();
    }
    @Override
    public Map<String, Object> otvW(Map<String, Object> parameters) {
        return initWriteTransactions(parameters);
    }
    @Override
    public Map<String, Object> otvR(Map<String, Object> parameters) {
        return initReadTransactions(parameters);
    }
    @Override
    public void frInit() {
        initData();
    }
    @Override
    public Map<String, Object> frW(Map<String, Object> parameters) {
        return initWriteTransactions(parameters);
    }
    @Override
    public Map<String, Object> frR(Map<String, Object> parameters) {
        return initReadTransactions(parameters);
    }

    private void initData() {
        GStoreTransaction conn = startTransaction();
        try{
            for (int i = 1; i <= 4; i++) {
                conn.execute("insert data {" +
                                "     <pers" + i + "> <version> 0  ." +
                                "     <pers" + i + "> <id> " + i + "  ." +
                                "     <pers" + i + "> <knows>  <pers" + (i == 4 ? 1 : i + 1) + "> ." +
                                "     <pers" + (i == 4 ? 1 : i + 1) + "> <knows>  <pers" + i + "> ." +
                                "}");
            }
            commitTransaction(conn);;
            //   conn.checkpoint();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> initWriteTransactions(Map<String, Object> parameters) {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int max = (int) parameters.get("cycleSize") + 1;
            long personId = random.nextInt(max - 1) + 1;
            GStoreTransaction conn = startTransaction();
            long pVersionIncreased;
            try{
                Result result = conn.execute("select ?v where {\n" +
                                "    ?p <id> \"" + personId + "\"  .\n" +
                                "    ?p <version> ?v .\n" +
                                "}");
                if(result.records().size()==0) {
                    throw new IllegalStateException("Missing person from query result.");
                }
                Record rc=result.records().get(0);
                pVersionIncreased  =rc.get(rc.index("v")).asLong() + 1;
                conn.execute("delete {\n" +
                                "    ?p1 <version> ?v1 .\n" +
                                "    ?p2 <version> ?v2 .\n" +
                                "    ?p3 <version> ?v3 .\n" +
                                "    ?p4 <version> ?v4 .\n" +
                                "}\n" +
                                "insert {\n" +
                                "    ?p1 <version> " + pVersionIncreased   +
                                "    ?p2 <version> " + pVersionIncreased   +
                                "    ?p3 <version> " + pVersionIncreased   +
                                "    ?p4 <version> " + pVersionIncreased   +
                                "} where {\n" +
                                "    ?p1 <id> " + personId   +
                                "    ?p1 <knows> ?p2 ;\n" +
                                "        <version> ?v1 .\n" +
                                "    ?p2 <knows> ?p3 ;\n" +
                                "        <version> ?v2 .\n" +
                                "    ?p3 <knows> ?p4 ;\n" +
                                "        <version> ?v3 .\n" +
                                "    ?p4 <knows> ?p1 ;\n" +
                                "        <version> ?v4 .\n" +
                                "}");
                commitTransaction(conn);;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return ImmutableMap.of();
    }

    private Map<String, Object> initReadTransactions(Map<String, Object> parameters) {
        final List<Object> firstRead = new ArrayList<>();
        final List<Object> secondRead = new ArrayList<>();
        GStoreTransaction conn = startTransaction();

        try {
            Result result = conn.execute(substituteParameters(
                    "select ?v1 ?v2 ?v3 ?v4 where {\n" +
                            "    ?p1 <id> %personId%  .\n" +
                            "    ?p1 <knows> ?p2 ; <version> ?v1 .\n" +
                            "    ?p2 <knows> ?p3 ; <version> ?v2 .\n" +
                            "    ?p3 <knows> ?p4 ; <version> ?v3 .\n" +
                            "    ?p4 <knows> ?p1 ; <version> ?v4 .\n" +
                            "}", parameters));
            if(result.records().size()==0) {
                throw new IllegalStateException("Missing query result.");
            }
            Record rc=result.records().get(0);
            firstRead.add(rc.get(rc.index("v1")).asLong());
            firstRead.add(rc.get(rc.index("v2")).asLong());
            firstRead.add(rc.get(rc.index("v3")).asLong());
            firstRead.add(rc.get(rc.index("v4")).asLong());

            sleep((Long) parameters.get("sleepTime"));
            Result result2 = conn.execute(substituteParameters(
                    "select ?v1 ?v2 ?v3 ?v4 where {\n" +
                            "    ?p1 <id> %personId%  .\n" +
                            "    ?p1 <knows> ?p2 ; <version> ?v1 .\n" +
                            "    ?p2 <knows> ?p3 ; <version> ?v2 .\n" +
                            "    ?p3 <knows> ?p4 ; <version> ?v3 .\n" +
                            "    ?p4 <knows> ?p1 ; <version> ?v4 .\n" +
                            "}", parameters));
            if(result2.records().size()==0) {
                throw new IllegalStateException("Missing query result.");
            }
            Record rc2=result2.records().get(0);

            System.out.println("v1"+rc2.get(rc2.index("v1")));
            System.out.println("v2"+rc2.get(rc2.index("v2")).asLong());
            System.out.println("v3"+rc2.get(rc2.index("v3")).asLong());
            System.out.println("v4"+rc2.get(rc2.index("v4")).asLong());


            secondRead.add(rc2.get(rc2.index("v1")).asLong());
            secondRead.add(rc2.get(rc2.index("v2")).asLong());
            secondRead.add(rc2.get(rc2.index("v3")).asLong());
            secondRead.add(rc2.get(rc2.index("v4")).asLong());
            commitTransaction(conn);;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    /**
     * Cut Anomalies: Lost Update (LU) anomaly
     */
    @Override
    public void luInit() {
        executeUpdate("insert data {\n" +
                "<pers1> <id> \"1\"  ;\n" +
                "        <type> <Person> ;\n" +
                "        <numFriends> \"0\"  .\n" +
                "}");
    }
    @Override
    public Map<String, Object> luW(Map<String, Object> parameters) {
        GStoreTransaction conn = startTransaction();
        try{
            conn.execute(substituteParameters(
                    "delete {\n" +
                            "     <pers%person1Id%> <numFriends> ?n .\n" +
                            "} insert {\n" +
                            "     <pers%person1Id%> <knows>  <pers%person2Id%> .\n" +
                            "     <pers%person1Id%> <numFriends> ?incrFr .\n" +
                            "} where {\n" +
                            "     <pers%person1Id%> <numFriends> ?n .\n" +
                            "    bind(?n + 1 as ?incrFr) .\n" +
                            "}", parameters));
            commitTransaction(conn);;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> luR(Map<String, Object> parameters) {
        long numKnowsEdges = 0;
        long numFriends = 0;
        GStoreTransaction conn = startTransaction();
        try {
            Result result = conn.execute(substituteParameters(
                    "select (count (<knows>) as ?numKnowsEdges) ?numFriendsProp where {\n" +
                            "    ?p <id> \"%personId%\"  .\n" +
                            "    ?p <knows> ?fr .\n" +
                            "    ?p  <numFriends> ?numFriendsProp . \n" +
                            "} group by ?p ?numFriendsProp", parameters));
            if(result.records().size()==0) {
                throw new IllegalStateException("OTV missing query result.");
            }
            Record rc=result.records().get(0);
            numKnowsEdges = rc.get(rc.index("numKnowsEdges")).asLong();
            numFriends = rc.get(rc.index("numFriendsProp")).asLong();

            commitTransaction(conn);;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of("numKnowsEdges", numKnowsEdges, "numFriendsProp", numFriends);
    }

    /**
     * Cut Anomalies: Write Skew (WS) anomaly
     */
    @Override
    public void wsInit() {
        GStoreTransaction conn = startTransaction();
        try{
            for (int i = 1; i <= 10; i++) {
                conn.execute(substituteParameters("insert data {\n" +
                        "     <pers%person1Id%> <id> \"%person1Id%\"  ;\n" +
                        "             <value> \"70\"  .\n" +
                        "     <pers%person2Id%> <id> \"%person2Id%\"  ;\n" +
                        "             <value> \"80\"  .\n" +
                        "}", ImmutableMap.of("person1Id", 2 * i - 1, "person2Id", 2 * i)));
            }
            commitTransaction(conn);;
            //     conn.checkpoint();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public Map<String, Object> wsW(Map<String, Object> parameters) {
        GStoreTransaction conn = startTransaction();
        try{
            Result result = conn.getConn().execute(conn.getTid(),substituteParameters(
                    "select ?p1Id ?p2Id where {\n" +
                            "     <pers%person1Id%> <version> ?p1v ;\n" +
                            "             <id> ?p1Id .\n" +
                            "     <pers%person2Id%> <version> ?p2v ;\n" +
                            "             <id> ?p2Id .\n" +
                            "    filter (?p1v + ?p2v >= 0)\n" +
                            "}", parameters));
            if(result.records().size()>0) {
                sleep((Long) parameters.get("sleepTime"));

                long personId = new Random().nextBoolean() ?
                        (long) parameters.get("person1Id") :
                        (long) parameters.get("person2Id");

                conn.getConn().execute(conn.getTid(), substituteParameters(
                        "delete {\n" +
                                "   <pers%personId%> <value> ?v .  \n" +
                                "} insert {\n" +
                                "   <pers%personId%> <value> ?newV .  \n" +
                                "} where {\n" +
                                "    <pers%personId%>  <value> ?v .  \n" +
                                "    bind(?v - 100 as ?newV)\n" +
                                "}", ImmutableMap.of("personId", personId)));
                commitTransaction(conn);
            }

            commitTransaction(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> wsR(Map<String, Object> parameters) {
        GStoreTransaction conn = startTransaction();
        try {
            Result result=  conn.getConn().execute(conn.getTid(),substituteParameters(
                    "select ?p1id ?p1value ?p2value ?p2id where {\n" +
                            "    ?p1 <id> ?p1id ;\n" +
                            "              <value> ?p1value .       \n" +
                            "    bind(?p1id + 1 as ?p2id) .\n" +
                            "    ?p2 <id> ?p2id ;\n" +
                            "              <value> ?p2value .              \n" +
                            "    filter (?p1value + ?p2value <= 0) .\n" +
                            "}", parameters));
            System.out.println("result records"+result.records().size());
            if(result.records().size()>0) {
                Record rc=result.records().get(0);
                // final BindingSet next = queryResult.next();
                return ImmutableMap.of(
                        "p1id", rc.get(rc.index("p1id")).asLong(),
                        "p1value",rc.get(rc.index("p1value")).asLong(),
                        "p2id", rc.get(rc.index("p2id")).asLong(),
                        "p2value", rc.get(rc.index("p2value")).asLong());
            }
            commitTransaction(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of();
    }
    @Override
    public GStoreTransaction startTransaction() {
        try {
            final GStoreConnector connector= this.connector;
            Result rbegin = connector.begin(isoLevel);
            String tid = rbegin.getNum("TID");
            System.out.println("start tid=" + tid);
            return new GStoreTransaction(tid,connector);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitTransaction(GStoreTransaction tt) throws Exception {
        tt.commit();
    }

    @Override
    public void abortTransaction(GStoreTransaction tt) throws Exception {
        tt.rollback();
    }

    @Override
    public BindingSet runQuery(GStoreTransaction tt, String querySpecification, Map<String, Object> stringObjectMap) throws Exception {
        return null;
    }

    @Override
    public void nukeDatabase() {
        try {
            GStoreTransaction conn=startTransaction();
            conn.getConn().execute(conn.getTid(), "DELETE WHERE { ?s ?p ?o }");
            commitTransaction(conn);
            //  conn.checkpoint();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void executeUpdate(String querySpecification) {
        try {
            GStoreTransaction conn = startTransaction();

            conn.getConn().execute(conn.getTid(),querySpecification);
            System.out.println("execute update ");
            commitTransaction(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void executeUpdate_checkpoint(String querySpecification) {
        try {
            GStoreTransaction conn = startTransaction();
            conn.getConn().execute(conn.getTid(),querySpecification);
            System.out.println("execute update");
            commitTransaction(conn);
            conn.getConn().checkpoint();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static String substituteParameters(String querySpecification, Map<String, Object> stringStringMap) {
        if (stringStringMap != null) {
            for (Map.Entry<String, Object> param : stringStringMap.entrySet()) {
                querySpecification = querySpecification.replace("%" + param.getKey() + "%", param.getValue().toString());
            }
        }
        return querySpecification;
    }

    @Override
    public void close() throws Exception {

    }
}
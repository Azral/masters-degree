/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.mongodb.BasicDBObject;
import extended.cassandra.CassandraQueryResult;
import extended.model.OWLOntologyCassandraStore;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Арнольд
 */
public class OWLOntologyCassandraStoreImpl implements OWLOntologyCassandraStore {

    static Cluster cluster;
    static Session session;
    public static int mergeFlag = 0;
    static String base;
    public String nameSpace;
    HashMap<String, Set<String>> created = new HashMap<>();
    long nextKey;

    public OWLOntologyCassandraStoreImpl(String ip, String namespace) throws UnknownHostException {
        if (cluster == null || cluster.isClosed()) {
            cluster = Cluster.builder().addContactPoint(ip).build();
        }
        if (session == null || session.isClosed()) {
            session = cluster.connect(namespace);
        }
    }

    public OWLOntologyCassandraStoreImpl(String nameSpace) throws UnknownHostException {
        this.nameSpace = nameSpace;
        if (cluster == null || cluster.isClosed()) {
            cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        }
        if (session == null || session.isClosed()) {
            session = cluster.connect(nameSpace);
        }
        //session.execute("drop KEYSPACE " + nameSpace + ";");
        try {
            session.execute("CREATE KEYSPACE " + nameSpace + " WITH replication "
                    + "= {'class':'SimpleStrategy', 'replication_factor':3};");
        } catch (Exception e) {
            // e.printStackTrace();
            //Statement statement = QueryBuilder.select().all().from("system", "schema_columns").where(new Clause eq("keyspace_name", nameSpace));
            List<Row> rows = session.execute("SELECT * FROM system.schema_columns where keyspace_name = '" + nameSpace + "';").all();
            for (Row row : rows) {

                Set<String> cols = created.get(row.getString("columnfamily_name"));
                if (cols != null) {
                    cols.add(row.getString("column_name"));
                } else {
                    cols = new HashSet<>();
                    cols.add(row.getString("column_name"));
                    created.put(row.getString("columnfamily_name"), cols);
                }
            }
            if (!created.containsKey("surrogatekeys")) {
                session.execute("CREATE TABLE " + nameSpace + ".SurrogateKeys ( uid bigint, PRIMARY KEY(uid));");
            }
            List<Row> keyRows = session.execute("SELECT * FROM " + nameSpace + ".SurrogateKeys;").all();
            long max = 1;
            for (Row row : keyRows) {
                if (row.getLong("uid") > max) {
                    max = row.getLong("uid");
                }
            }
            nextKey = max + 1;
        }
    }

    public OWLOntologyCassandraStoreImpl(String nameSpace, boolean drop) throws UnknownHostException {
        this.nameSpace = nameSpace;
        if (cluster == null || cluster.isClosed()) {
            cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        }
        if (session == null || session.isClosed()) {
            try {
                session = cluster.connect(nameSpace);
            } catch (Exception e) {
                session = cluster.connect();
            }
        }
        if (drop) {
            try {
                session.execute("drop KEYSPACE " + nameSpace + ";");
            } catch (Exception e) {

            }
        }
        try {
            session.execute("CREATE KEYSPACE " + nameSpace + " WITH replication "
                    + "= {'class':'SimpleStrategy', 'replication_factor':3};");
            session = cluster.connect(nameSpace);
        } catch (Exception e) {
            //session = cluster.connect(nameSpace);
            // e.printStackTrace();
            //Statement statement = QueryBuilder.select().all().from("system", "schema_columns").where(new Clause eq("keyspace_name", nameSpace));
            List<Row> rows = session.execute("SELECT * FROM system.schema_columns where keyspace_name = '" + nameSpace + "';").all();
            for (Row row : rows) {

                Set<String> cols = created.get(row.getString("columnfamily_name"));
                if (cols != null) {
                    cols.add(row.getString("column_name"));
                } else {
                    cols = new HashSet<>();
                    cols.add(row.getString("column_name"));
                    created.put(row.getString("columnfamily_name"), cols);
                }
            }
            if (!created.containsKey("surrogatekeys")) {
                session.execute("CREATE TABLE " + nameSpace + ".SurrogateKeys ( uid bigint, PRIMARY KEY(uid));");
            }
            List<Row> keyRows = session.execute("SELECT * FROM " + nameSpace + ".SurrogateKeys;").all();
            long max = 1;
            for (Row row : keyRows) {
                if (row.getLong("uid") > max) {
                    max = row.getLong("uid");
                }
            }
            nextKey = max + 1;
        }
    }

    public long countIndividuals(String query) {
        if (query.equals("fullSearch")) {
            return Long.MAX_VALUE;
        }
        if (query.startsWith("TwoTableSearchselect")) {
            long firstTableCount = 0;
            long secondTableCount = 0;
            int count = 0;
            String firstQuery = query.replaceAll("^TwoTableSearchselect .* from \"(.+?)\"", "select count(*) from \"$1_rs\"");
            firstQuery += "ALLOW FILTERING";
            String secondQuery = query.replaceAll("^TwoTableSearchselect .* from \"(.+?)\"", "select count(*) from \"$1_txt\"");
            secondQuery += "ALLOW FILTERING";
            try {
                firstTableCount = session.execute(firstQuery).one().getLong(0);
            } catch (Exception e) {
                // e.printStackTrace();
                count++;
            }
            try {
                secondTableCount = session.execute(secondQuery).one().getLong(0);
            } catch (Exception e) {
                count++;
            }
            if (count == 2) {
                return 0;
            } else {
                return firstTableCount + secondTableCount;
            }
        } else {
            return session.execute(query).one().getLong(0);
        }
        //return 1;
    }

    //public String setQueryName
    public List<CassandraQueryResult> findNode(String query) {
        List<CassandraQueryResult> rs = new ArrayList<>();
        if (query.equals("fullSearch")) {
            return null;
        }
        String tableName = null;
        if (query.startsWith("TwoTableSearchselect")) {
            tableName = query.replaceAll("^TwoTableSearchselect .* from \"(.+?)\".*$", "$1");
            String firstQuery = query.replaceAll("^TwoTableSearchselect .* from \"(.+?)\"", "select * from \"$1_rs\"");
            firstQuery += "ALLOW FILTERING";
            String secondQuery = query.replaceAll("^TwoTableSearchselect .* from \"(.+?)\"", "select * from \"$1_txt\"");
            secondQuery += "ALLOW FILTERING";
            try {
                rs.add(new CassandraQueryResult(session.execute(firstQuery), tableName));
            } catch (Exception e) {
                //e.printStackTrace();
            }
            try {
                rs.add(new CassandraQueryResult(session.execute(secondQuery), tableName));
            } catch (Exception e) {
                //e.printStackTrace();
            }
        } else {
            tableName = query.replaceAll("^select .* from \"(.+?)\".*$", "$1");
            try {
                rs.add(new CassandraQueryResult(session.execute(query + " ALLOW FILTERING"), tableName));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return rs;
    }

    public static void setBase(String iri) {
        base = iri;
    }

    @Override
    public void clearDB() {
        session.execute("drop KEYSPACE " + nameSpace + ";");
    }

    public boolean isOnlyText(BasicDBObject bObject) {
        boolean result = true;
        for (String key : bObject.keySet()) {
            if (!key.equals("source") && !key.equals("textContent")) {
                result = false;
            }
        }
        return result;
    }

    public void reviewTableColumns(String tableName, Set<String> cols) {
        String alterTable;
        if (created.get(tableName) == null) {
            return;
        }
        Set<String> existCols = new HashSet<>(created.get(tableName));
        for (String col : cols) {
            if (!existCols.contains(col)) {
                alterTable = "ALTER TABLE " + nameSpace + ".\"" + tableName + "\" ADD \"" + col + "\" text";
                existCols.add(col);
                //System.out.println(alterTable);
                session.execute(alterTable);
            }
        }
        created.put(tableName, existCols);

    }

    public String getSurrogateKey() {
        String s = "SurrogateKey_" + nextKey;
        nextKey++;
        return s;
    }

    public void createTable(Set<String> columnSet, String tableName) {
        tableName = reviewTableName(tableName, columnSet);
        reviewTableColumns(tableName, columnSet);
        if (created.get(tableName) == null && !tableName.isEmpty()) {
            String createStm = "CREATE TABLE " + nameSpace + ".\"" + tableName + "\" (";
            String primaryKey = "";
            for (String col : columnSet) {
                createStm += "\"" + col + "\"" + " text,";
                primaryKey += "\"" + col + "\"" + ",";
            }
            primaryKey = primaryKey.substring(0, primaryKey.length() - 1);
            if (columnSet.contains("rdf:about")) {

                createStm += "PRIMARY KEY (\"rdf:about\")";
            } else {
                createStm += "PRIMARY KEY (" + primaryKey + ")";
            }
            createStm += ");";
            //System.out.println(createStm);
            session.execute(createStm);
            created.put(tableName, columnSet);
        }
    }

    public String reviewTableName(String tableName, Set<String> columns) {
        tableName = tableName.replaceAll(":", "_");
        if (columns.contains("textContent")) {
            tableName = tableName + "_txt";
        } else {
            tableName = tableName + "_rs";
        }
        return tableName;
    }

    public void insertRow(HashMap<String, String> data, String tableName) {
        tableName = reviewTableName(tableName, data.keySet());
        String insertStm = "insert into " + nameSpace + ".\"" + tableName + "\" (";
        String insertCols = "";
        String insertVals = "";
        for (String key : data.keySet()) {
            insertCols += "\"" + key + "\",";
            insertVals += "\'" + data.get(key) + "\',";
        }
        insertCols = insertCols.substring(0, insertCols.length() - 1);
        insertVals = insertVals.substring(0, insertVals.length() - 1);
        insertStm += insertCols + ") values (";
        insertStm += insertVals + ");";
        //System.out.println(insertStm);
        session.executeAsync(insertStm);
    }

    public void write(String text) {

    }

    public void write(char c) {

    }

    public void flush() {
        /* if (!session.isClosed()) {
         session.close();
         }
         if (!cluster.isClosed()) {
         cluster.close();
         }*/
    }

    public static void close() {
        if (!session.isClosed()) {
            session.close();
        }
        if (!cluster.isClosed()) {
            cluster.close();
        }
    }

    public void drop() {
        session.execute("drop KEYSPACE " + nameSpace + ";");
    }

    /*@Override
     protected void finalize() throws Throwable {
     if (!session.isClosed()) {
     session.close();
     }
     if (!cluster.isClosed()) {
     cluster.close();
     }
     super.finalize(); //To change body of generated methods, choose Tools | Templates.
     }*/
}

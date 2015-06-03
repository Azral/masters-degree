/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.model;

import java.util.HashMap;
import java.util.Set;
import com.datastax.driver.core.ResultSet;
import extended.cassandra.CassandraQueryResult;
import java.util.List;

/**
 *
 * @author Арнольд
 */
public interface OWLOntologyCassandraStore {

    public void write(String text);

    public void clearDB();

    public String getSurrogateKey();

    public void write(char c);

    public void createTable(Set<String> columnSet, String tableName);

    public void insertRow(HashMap<String, String> data, String tableName);

    public void flush();

    public long countIndividuals(String query);

    public List<CassandraQueryResult> findNode(String query);
    
    //public static void close();

}

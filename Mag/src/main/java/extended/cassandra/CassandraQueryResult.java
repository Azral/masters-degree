/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra;

import com.datastax.driver.core.ResultSet;

/**
 *
 * @author Арнольд
 */
public class CassandraQueryResult {

    private ResultSet rs;
    private String tableName;

    public CassandraQueryResult(ResultSet rs, String tableName) {
        this.rs = rs;
        this.tableName = tableName;
    }

    public ResultSet getRs() {
        return rs;
    }

    public String getTableName() {
        return tableName;
    }

}

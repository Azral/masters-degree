/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.apibindings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author Арнольд
 */
public class SPARQLRow {

    private HashMap<String, String> row;

    public SPARQLRow() {
        row = new HashMap<>();
    }

    public SPARQLRow(SPARQLRow lRow) {
        row = (HashMap<String, String>) lRow.row.clone();
    }

    public SPARQLRow(SPARQLRow lRow, SPARQLRow rRow) {
        row = (HashMap<String, String>) lRow.row.clone();
        for (String key : rRow.getRow().keySet()) {
            if (!row.containsKey(key)) {
                row.put(key, rRow.getRow().get(key));
            }
        }
    }

    public SPARQLRow getColumns() {
        SPARQLRow newRow = new SPARQLRow();
        for (String keys : row.keySet()) {
            newRow.putData(keys, null);
        }
        return newRow;
    }

    public List<String> getRelations(SPARQLRow r) {
        List<String> cols = new ArrayList<>();
        for (String key : row.keySet()) {
            if (r.getRow().containsKey(key)) {
                cols.add(key);
            }
        }
        return cols;
    }

    public void putData(String key, String data) {
        row.put(key, data);
    }

    public HashMap<String, String> getRow() {
        return row;
    }

    public HashMap<String, String> getRelations(List<String> joinKeys) {
        HashMap<String, String> relatedMap = new HashMap<>();
        for (String joinKey : joinKeys) {
            relatedMap.put(joinKey, row.get(joinKey));
        }
        return relatedMap;
    }

    public boolean containsRow(SPARQLRow r) {
        // boolean flag;
        for (String key : row.keySet()) {
            //if 
            if ((r.getRow().get(key) != null && row.get(key) == null) || (r.getRow().get(key) == null && row.get(key) != null) || !r.getRow().get(key).equals(row.get(key))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SPARQLRow) {
            SPARQLRow r = (SPARQLRow) obj;
            if (this.getRow().equals(r.getRow())) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.row);
        return hash;
    }

    @Override
    public String toString() {
        String result = "";
        for (String data : row.values()) {
            result += data + "\t";
        }
        return result;
    }

}

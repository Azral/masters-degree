/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.apibindings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Арнольд
 */
public class SPARQLTable {

    private HashMap<HashMap<String, String>, Set<SPARQLRow>> list = new HashMap<>();
    private List<String> columns = new ArrayList<>();

    public SPARQLTable() {

    }

    public void addRow(SPARQLRow row) {
        Set<SPARQLRow> s;
        for (String key : row.getRow().keySet()) {
            HashMap<String, String> k = new HashMap<>();
            k.put(key, row.getRow().get(key));
            //if (row.getRow().get(key).equals("http://www.Department9.University10.edu/Course16")){
            //    int i = 1+1;
            //}
            if (!columns.contains(key)) {
                columns.add(key);
            }
            s = list.get(k);
            if (s == null) {
                s = new HashSet<>();
                list.put(k, s);
            }
            s.add(row);

        }

    }

    public List<String> getColumnList() {
        return columns;
    }

    public List<SPARQLRow> getTableList() {
        Set<SPARQLRow> s = new HashSet<>();
        for (HashMap<String, String> key : list.keySet()) {
            s.addAll(list.get(key));
        }
        return new ArrayList<>(s);
    }

    public SPARQLRow getColumns() {
        SPARQLRow newRow = new SPARQLRow();
        for (String keys : columns) {
            newRow.putData(keys, null);
        }
        return newRow;
    }

    public void removeRow(SPARQLRow row) {
        Set<SPARQLRow> s;
        for (String key : row.getRow().keySet()) {
            HashMap<String, String> k = new HashMap<>();
            k.put(key, row.getRow().get(key));
            if (columns.isEmpty()) {
                columns.add(key);
            }
            s = list.get(k);
            if (s != null) {
                s.remove(row);
                if (s.isEmpty()) {
                    list.remove(k);
                }
            }

        }
    }

    public HashMap<HashMap<String, String>, Set<SPARQLRow>> getTable() {
        return list;
    }

    public List<String> getRelations(SPARQLTable table) {
        List<String> relations = new ArrayList<>();
        for (String key : columns) {
            if (table.getColumnList().contains(key)) {
                relations.add(key);
            }
        }
        return relations;
    }

    public void addData(String key, String data) {
        SPARQLRow newRow = null;
        SPARQLTable t = new SPARQLTable();
        t.list = new HashMap<>(list);
        //HashMap<HashMap<String, String>, Set<SPARQLRow>> templist = new HashMap<>(list);
        //List<SPARQLRow> templist = new ArrayList<>(list);
        if (!list.isEmpty()) {
            //list.
            for (SPARQLRow lRow : getTableList()) {
                if (lRow.getRow().containsKey(key) && !lRow.getRow().get(key).equals(data)) {
                    newRow = new SPARQLRow(lRow);
                    newRow.getRow().put(key, data);
                } else {
                    lRow.putData(key, data);
                    t.addRow(lRow);
                }
                if (newRow != null) {
                    t.addRow(newRow);
                }
            }
            list = t.list;
            columns = t.columns;
        } else {
            newRow = new SPARQLRow();
            newRow.putData(key, data);
            addRow(newRow);
        }

    }

    /*public void addData(String key, String data) {
     SPARQLRow newRow = null;
     List<SPARQLRow> templist = new ArrayList<>(list);
     if (!list.isEmpty()) {
     //list.
     for (SPARQLRow lRow : list) {
     if (lRow.getRow().containsKey(key) && !lRow.getRow().get(key).equals(data)) {
     newRow = new SPARQLRow(lRow);
     newRow.getRow().put(key, data);
     } else {
     lRow.putData(key, data);
     }
     if (newRow != null && !templist.contains(newRow)) {
     templist.add(newRow);
     }
     }
     list = templist;
     } else {
     newRow = new SPARQLRow();
     newRow.putData(key, data);
     list.add(newRow);
     }

     }*/
    public List<SPARQLRow> getListByRelations(HashMap<String, String> datas) {
        Set<SPARQLRow> leftSet = null;
        boolean checked = false;
        Set<SPARQLRow> leftSetCopy;
        List<Set<SPARQLRow>> res = new ArrayList<>();
        //boolean rowCompatible;
        for (String key : datas.keySet()) {
            HashMap<String, String> k = new HashMap<>();
            k.put(key, datas.get(key));
            if (leftSet == null && checked == false) {
                leftSet = list.get(k);
                checked = true;
            } else {
                res.add(list.get(k));
            }
        }
        if (leftSet == null) {
            return new ArrayList<>();
        }
        leftSetCopy = new HashSet<>(leftSet);
        if (!res.isEmpty()) {
            for (SPARQLRow r : leftSet) {
                //if (res.)
                for (Set<SPARQLRow> s : res) {
                    if (s != null && !s.contains(r)) {
                        leftSetCopy.remove(r);
                    }
                    if (s == null){
                        leftSetCopy.clear();
                        return new ArrayList<>();
                    }
                    
                }
            }
        }

        return new ArrayList<>(leftSetCopy);
    }

    public List<SPARQLRow> getListByData(String key, String data) {
        HashMap<String, String> k = new HashMap<>();
        k.put(key, data);
        return new ArrayList<>(list.get(k));
    }

    public void addPartition(SPARQLTable lTable) {
        for (SPARQLRow r : lTable.getTableList()) {
            addRow(r);
        }
        //list.putAll(lTable.getTable());
        columns = lTable.columns;
    }

    @Override
    public String toString() {
        Set<SPARQLRow> s = new HashSet<>();
        List<SPARQLRow> l;
        String result = "";
        if (list.isEmpty()) {
            return result;
        }
        for (HashMap<String, String> key : list.keySet()) {
            s.addAll(list.get(key));
        }
        l = new ArrayList<>(s);

        if (l.get(0) != null) {
            for (String key : l.get(0).getRow().keySet()) {
                result += key + "\t";
            }
            result += "\n";
        }

        for (SPARQLRow row : l) {
            result += row.toString() + "\n";
        }
        return result;
    }
}

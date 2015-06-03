/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra.SPARQLQuery;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import extended.apibindings.SPARQLRow;
import extended.apibindings.SPARQLTable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import extended.cassandra.CassandraQueryResult;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

/**
 *
 * @author Арнольд
 */
public class CassandraSPARQLQuery {

    private List<CassandraQueryNode> mqns = new ArrayList<>();
    private TupleExpr expr;
    private Map<String, String> prefixes;
    private SPARQLTable data;
    private long count;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public CassandraSPARQLQuery() {
    }

    public Map<String, String> getPrefixes() {
        return prefixes;
    }

    public void setPrefixes(Map<String, String> prefixes) {
        this.prefixes = prefixes;
    }

    public SPARQLTable getData() {
        return data;
    }

    public void setData(SPARQLTable data) {
        this.data = data;
    }

    public void setTupleExpr(TupleExpr expr) {
        this.expr = expr;
    }

    public boolean isSimple() {
        for (CassandraQueryNode n : mqns) {
            if (n.getCount() < 100) {
                return true;
            }
        }
        return false;
    }

    public void addStatement(StatementPattern sp, String type) {
        //if (mqns.isEmpty()) {
        mqns.add(new CassandraQueryNode(sp, type));
        /* } else {
         CassandraQueryNode node = matchNode(sp);
         if (node == null) {
         mqns.add(new CassandraQueryNode(sp, type));
         } else {
         node.addAttribute(new CassandraQueryNodeAttribute(sp, type));
         }
         }*/
    }

    public void clearData() {
        mqns.stream().forEach((n) -> {
            n.setData(null);
        });
    }

    public void clearUnused() {
        for (CassandraQueryNode n : mqns) {
            if (getChildNodes(n).isEmpty() && getNonVisitedParentNodes(n).isEmpty()) {
                n.setData(null);
            }
        }
    }

    public boolean isLastNode() {
        int count = 0;
        for (CassandraQueryNode n : mqns) {
            if (n.getData() == null) {
                count++;
            }
        }
        if (count == 1) {
            return true;
        } else {
            return false;
        }
    }

    public List<CassandraQueryNode> getNonVisitedNodes() {
        List<CassandraQueryNode> result = new ArrayList<>();
        for (CassandraQueryNode n : mqns) {
            if (!n.isVisited()) {
                result.add(n);
            }
        }
        return result;
    }

    public List<String> getAttrRelationList(CassandraQueryNode node) {
        List<String> result = new ArrayList<>();
        for (CassandraQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (CassandraQueryNodeAttribute attributeright : node.getAttributes()) {
                if (!attributeleft.equals(attributeright)) {
                    if (attributeleft.getSubjectVal() == null && attributeleft.getSubject().getName().equals(attributeright.getSubject().getName()) && !result.contains(attributeleft.getSubject().getName())) {
                        result.add(attributeleft.getSubject().getName());
                    }
                    if (attributeleft.getSubjectVal() == null && attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && !result.contains(attributeleft.getSubject().getName())) {
                        result.add(attributeleft.getSubject().getName());
                    }
                    if (attributeleft.getSubjectVal() == null && attributeleft.getSubject().getName().equals(attributeright.getObject().getName()) && !result.contains(attributeleft.getSubject().getName())) {
                        result.add(attributeleft.getSubject().getName());
                    }
                    if (attributeleft.getPredicateVal() == null && attributeleft.getPredicate().getName().equals(attributeright.getSubject().getName()) && !result.contains(attributeleft.getPredicate().getName())) {
                        result.add(attributeleft.getPredicate().getName());
                    }
                    if (attributeleft.getPredicateVal() == null && attributeleft.getPredicate().getName().equals(attributeright.getPredicate().getName()) && !result.contains(attributeleft.getPredicate().getName())) {
                        result.add(attributeleft.getPredicate().getName());
                    }
                    if (attributeleft.getPredicateVal() == null && attributeleft.getPredicate().getName().equals(attributeright.getObject().getName()) && !result.contains(attributeleft.getPredicate().getName())) {
                        result.add(attributeleft.getPredicate().getName());
                    }
                    if (attributeleft.getObjectVal() == null && attributeleft.getObject().getName().equals(attributeright.getSubject().getName()) && !result.contains(attributeleft.getObject().getName())) {
                        result.add(attributeleft.getObject().getName());
                    }
                    if (attributeleft.getObjectVal() == null && attributeleft.getObject().getName().equals(attributeright.getPredicate().getName()) && !result.contains(attributeleft.getObject().getName())) {
                        result.add(attributeleft.getObject().getName());
                    }
                    if (attributeleft.getObjectVal() == null && attributeleft.getObject().getName().equals(attributeright.getObject().getName()) && !result.contains(attributeleft.getObject().getName())) {
                        result.add(attributeleft.getObject().getName());
                    }
                }
            }
        }
        return result;
    }
   

    public List<CassandraQueryNode> getNonVisitedRelatedNodes(CassandraQueryNode node) {
        List<CassandraQueryNode> result = new ArrayList<>();

        for (CassandraQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (CassandraQueryNode noderight : getNonVisitedNodes()) {
                if (!noderight.equals(node)) {
                    for (CassandraQueryNodeAttribute attributeright : noderight.getAttributes()) {
                        if (attributeleft.getSubject().getName().equals(attributeright.getSubject().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getSubject().getName().equals(attributeright.getObject().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getObject().getName().equals(attributeright.getSubject().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                    }
                }
            }
        }

        return result;
    }

    public List<CassandraQueryNode> getParentNodes(CassandraQueryNode node) {
        List<CassandraQueryNode> result = new ArrayList<>();

        for (CassandraQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (CassandraQueryNode noderight : getNodes()) {
                if (!noderight.equals(node)) {
                    for (CassandraQueryNodeAttribute attributeright : noderight.getAttributes()) {
                        /*if (attributeleft.getSubject().getName().equals(attributeright.getSubject().getName()) && attributeleft.getSubjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getSubjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }*/
                        if (attributeleft.getSubject().getName().equals(attributeright.getObject().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        /*if (attributeleft.getPredicate().getName().equals(attributeright.getSubject().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getPredicate().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getPredicate().getName().equals(attributeright.getObject().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getSubject().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getObject().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }*/
                    }
                }
            }
        }

        return result;
    }

    public List<CassandraQueryNode> getNonVisitedParentNodes(CassandraQueryNode node) {
        List<CassandraQueryNode> result = new ArrayList<>();

        for (CassandraQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (CassandraQueryNode noderight : getNonVisitedNodes()) {
                if (!noderight.equals(node)) {
                    for (CassandraQueryNodeAttribute attributeright : noderight.getAttributes()) {
                        /*if (attributeleft.getSubject().getName().equals(attributeright.getSubject().getName()) && attributeleft.getSubjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getSubjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }*/
                        if (attributeleft.getSubject().getName().equals(attributeright.getObject().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        /*if (attributeleft.getPredicate().getName().equals(attributeright.getSubject().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getPredicate().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getPredicate().getName().equals(attributeright.getObject().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getSubject().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getObject().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }*/
                    }
                }
            }
        }

        return result;
    }

    public List<CassandraQueryNode> getChildNodes(CassandraQueryNode node) {
        List<CassandraQueryNode> result = new ArrayList<>();

        for (CassandraQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (CassandraQueryNode noderight : getNonVisitedNodes()) {
                if (!noderight.equals(node)) {
                    for (CassandraQueryNodeAttribute attributeright : noderight.getAttributes()) {
                        /*if (attributeleft.getSubject().getName().equals(attributeright.getSubject().getName()) && attributeleft.getSubjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getSubjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }*/
                        if (attributeleft.getObject().getName().equals(attributeright.getSubject().getName()) && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        /*if (attributeleft.getPredicate().getName().equals(attributeright.getSubject().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getPredicate().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getPredicate().getName().equals(attributeright.getObject().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getSubject().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }
                         if (attributeleft.getObject().getName().equals(attributeright.getObject().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }*/
                    }
                }
            }
        }

        return result;
    }

    public List<CassandraQueryNode> getRelatedNodes(CassandraQueryNode node) {
        List<CassandraQueryNode> result = new ArrayList<>();

        for (CassandraQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (CassandraQueryNode noderight : getNonVisitedNodes()) {
                if (!noderight.equals(node)) {
                    for (CassandraQueryNodeAttribute attributeright : noderight.getAttributes()) {
                        if (attributeleft.getSubject().getName().equals(attributeright.getSubject().getName()) && attributeleft.getSubjectVal() == null && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getSubjectVal() == null && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        /*if (attributeleft.getSubject().getName().equals(attributeright.getObject().getName()) && !result.contains(noderight)) {
                         result.add(noderight);
                         }*/
                        if (attributeleft.getPredicate().getName().equals(attributeright.getSubject().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getPredicate().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getPredicate().getName().equals(attributeright.getObject().getName()) && attributeleft.getPredicateVal() == null && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getObject().getName().equals(attributeright.getSubject().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        if (attributeleft.getObject().getName().equals(attributeright.getPredicate().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                            result.add(noderight);
                        }
                        /*if (attributeleft.getObject().getName().equals(attributeright.getObject().getName()) && attributeleft.getObjectVal() == null && !result.contains(noderight)) {
                         result.add(noderight);
                         }*/
                    }
                }
            }
        }

        return result;
    }

    public List<String> getNodeRelationList() {
        List<String> result = new ArrayList<>();
        for (CassandraQueryNode nodeleft : mqns) {
            for (CassandraQueryNodeAttribute attributeleft : nodeleft.getAttributes()) {
                for (CassandraQueryNode noderight : mqns) {
                    if (!noderight.equals(nodeleft)) {
                        for (CassandraQueryNodeAttribute attributeright : noderight.getAttributes()) {
                            if (attributeleft.getSubject().getName().equals(attributeright.getSubject().getName()) && !result.contains(attributeleft.getSubject().getName())) {
                                result.add(attributeleft.getSubject().getName());
                            }
                            if (attributeleft.getSubject().getName().equals(attributeright.getPredicate().getName()) && !result.contains(attributeleft.getSubject().getName())) {
                                result.add(attributeleft.getSubject().getName());
                            }
                            if (attributeleft.getSubject().getName().equals(attributeright.getObject().getName()) && !result.contains(attributeleft.getSubject().getName())) {
                                result.add(attributeleft.getSubject().getName());
                            }
                            if (attributeleft.getPredicate().getName().equals(attributeright.getSubject().getName()) && !result.contains(attributeleft.getPredicate().getName())) {
                                result.add(attributeleft.getPredicate().getName());
                            }
                            if (attributeleft.getPredicate().getName().equals(attributeright.getPredicate().getName()) && !result.contains(attributeleft.getPredicate().getName())) {
                                result.add(attributeleft.getPredicate().getName());
                            }
                            if (attributeleft.getPredicate().getName().equals(attributeright.getObject().getName()) && !result.contains(attributeleft.getPredicate().getName())) {
                                result.add(attributeleft.getPredicate().getName());
                            }
                            if (attributeleft.getObject().getName().equals(attributeright.getSubject().getName()) && !result.contains(attributeleft.getObject().getName())) {
                                result.add(attributeleft.getObject().getName());
                            }
                            if (attributeleft.getObject().getName().equals(attributeright.getPredicate().getName()) && !result.contains(attributeleft.getObject().getName())) {
                                result.add(attributeleft.getObject().getName());
                            }
                            if (attributeleft.getObject().getName().equals(attributeright.getObject().getName()) && !result.contains(attributeleft.getObject().getName())) {
                                result.add(attributeleft.getObject().getName());
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public boolean isNodeRelation(CassandraQueryNode node, String attr) {
        if (getNodeRelationList().contains(attr)) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isAttrRelation(CassandraQueryNode node, String attr) {
        if (getAttrRelationList(node).contains(attr)) {
            return true;
        } else {
            return false;
        }
    }

    public CassandraQueryNode matchNode(StatementPattern sp) {
        for (CassandraQueryNode mqn : mqns) {
            if (mqn.getNodeName().equals(sp.getVarList().get(0).getName())) {
                return mqn;
            }
        }
        return null;
    }

    public List<CassandraQueryNode> getNodes() {
        return mqns;
    }

    public TupleExpr getExpr() {
        return expr;
    }

    /* public SPARQLTable getJoinNodesTable(CassandraQueryNode node1, CassandraQueryNode node2, DBCursor cursor) {
     SPARQLTable result = new SPARQLTable();
     Iterator<DBObject> it = cursor.iterator();
     while (it.hasNext()) {
     SPARQLTable partition = new SPARQLTable();
     DBObject bObject = it.next();
     if (!node1.getTranslatableAttributes().isEmpty()) {
     for (CassandraQueryNodeAttribute el : node1.getTranslatableAttributes()) {
     if (el.getExprType().equals("Join")) {
     constructJoin(el, partition, bObject);
     reduceData(partition, el, node2);
     }
     }
     } else {
     return null;
     }
     result.addPartition(partition);
     }

     return result;
     }*/
    public void reduceData(SPARQLTable partition, CassandraQueryNodeAttribute el, CassandraQueryNode node2) {
        for (SPARQLRow r : node2.getData().getTableList()) {

        }
    }

    public SPARQLTable getTranslatableNodeTable(CassandraQueryNode node, List<CassandraQueryResult> cursor) {
        SPARQLTable result = new SPARQLTable();
        for (int i = 0; i < cursor.size(); i++) {

            Iterator<Row> it = cursor.get(i).getRs().iterator();
            // int count = 0;
            while (it.hasNext()) {

                // count++;
                //System.out.println(count);
                SPARQLTable partition = new SPARQLTable();
                Row row = it.next();
                //if (!node.getTranslatableAttributes().isEmpty()) {
                for (CassandraQueryNodeAttribute el : node.getTranslatableAttributes()) {
                    //if (el.getExprType().equals("Join")) {
                        constructJoin(el, partition, row, cursor.get(i).getTableName());
                   // }
                }
                //} else {
                // return null;
                //}*/
                result.addPartition(partition);
            }
        }

        return result;
    }
    /*public SPARQLTable getNonTranslatableNodeTable(CassandraQueryNode node, DBCursor cursor) {
     SPARQLTable result = new SPARQLTable();
     Iterator<DBObject> it = cursor.iterator();
     while (it.hasNext()) {
     SPARQLTable partition = new SPARQLTable();
     DBObject bObject = it.next();
     if (!node.getNonTranslatableAttributes().isEmpty()) {
     for (CassandraQueryNodeAttribute el : node.getNonTranslatableAttributes()) {
     if (el.getExprType().equals("Join")) {
     constructJoin(el, partition, bObject);
     }
     }
     } else {
     return null;
     }
     result.addPartition(partition);
     }

     return result;
     }*/

    public void constructJoin(CassandraQueryNodeAttribute el, SPARQLTable partition, Row row, String tableName) {
        if (el.getSubjectVal() == null) {
            partition.addData(el.getSubject().getName(), row.getString("parentAbout"));
        }
        if (el.getPredicateVal() == null) {
            partition.addData(el.getPredicate().getName(), decodeShort(tableName));
        }

        if (el.getObjectVal() == null) {
            String objData;
            objData = row.getString("rdf:resource");
            if (objData == null) {
                objData = row.getString("textContent");
            }
            partition.addData(el.getObject().getName(), objData);
        }
    }

    public String decodeShort(String value) {
        String newString = value;
        for (String key : getPrefixes().keySet()) {
            if (newString.contains(getPrefixes().get(key))) {
                newString = newString.replace(key + ":", getPrefixes().get(key));
            }
        }
        return newString;
    }

    public String replaceToShort(String value) {
        String newString = value;
        for (String key : getPrefixes().keySet()) {
            if (newString.contains(getPrefixes().get(key))) {
                newString = newString.replace(getPrefixes().get(key), key + ":");
            }
        }
        return newString;
    }

}

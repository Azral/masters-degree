/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import extended.apibindings.SPARQLRow;
import extended.apibindings.SPARQLTable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;

/**
 *
 * @author Арнольд
 */
public class MongoSPARQLQuery {

    private List<MongoQueryNode> mqns = new ArrayList<>();
    private TupleExpr expr;
    private Map<String, String> prefixes;
    private SPARQLTable data;

    public MongoSPARQLQuery() {
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
        for (MongoQueryNode n : mqns) {
            if (n.getCount() < 100) {
                return true;
            }
        }
        return false;
    }

    public void addStatement(StatementPattern sp, String type) {
        if (mqns.isEmpty()) {
            mqns.add(new MongoQueryNode(sp, type));
        } else {
            MongoQueryNode node = matchNode(sp);
            if (node == null) {
                mqns.add(new MongoQueryNode(sp, type));
            } else {
                node.addAttribute(new MongoQueryNodeAttribute(sp, type));
            }
        }
    }

    public void clearData() {
        mqns.stream().forEach((n) -> {
            n.setData(null);
        });
    }

    public void clearUnused() {
        for (MongoQueryNode n : mqns) {
            if (getChildNodes(n).isEmpty() && getVisitedParentNodes(n).isEmpty()) {
                n.setData(null);
            }
        }
    }

    public boolean isLastNode() {
        int count = 0;
        for (MongoQueryNode n : mqns) {
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

    public List<MongoQueryNode> getNonVisitedNodes() {
        List<MongoQueryNode> result = new ArrayList<>();
        for (MongoQueryNode n : mqns) {
            if (!n.isVisited()) {
                result.add(n);
            }
        }
        return result;
    }

    public List<String> getAttrRelationList(MongoQueryNode node) {
        List<String> result = new ArrayList<>();
        for (MongoQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (MongoQueryNodeAttribute attributeright : node.getAttributes()) {
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

    public List<MongoQueryNode> getParentNodes(MongoQueryNode node) {
        List<MongoQueryNode> result = new ArrayList<>();

        for (MongoQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (MongoQueryNode noderight : getNodes()) {
                if (!noderight.equals(node)) {
                    for (MongoQueryNodeAttribute attributeright : noderight.getAttributes()) {
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
    
    public List<MongoQueryNode> getNonVisitedParentNodes(MongoQueryNode node) {
        List<MongoQueryNode> result = new ArrayList<>();

        for (MongoQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (MongoQueryNode noderight : getNonVisitedNodes()) {
                if (!noderight.equals(node)) {
                    for (MongoQueryNodeAttribute attributeright : noderight.getAttributes()) {
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

    public List<MongoQueryNode> getVisitedParentNodes(MongoQueryNode node) {
        List<MongoQueryNode> result = new ArrayList<>();

        for (MongoQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (MongoQueryNode noderight : getNonVisitedNodes()) {
                if (!noderight.equals(node)) {
                    for (MongoQueryNodeAttribute attributeright : noderight.getAttributes()) {
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

    public List<MongoQueryNode> getChildNodes(MongoQueryNode node) {
        List<MongoQueryNode> result = new ArrayList<>();

        for (MongoQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (MongoQueryNode noderight : getNonVisitedNodes()) {
                if (!noderight.equals(node)) {
                    for (MongoQueryNodeAttribute attributeright : noderight.getAttributes()) {
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

    public List<MongoQueryNode> getRelatedNodes(MongoQueryNode node) {
        List<MongoQueryNode> result = new ArrayList<>();

        for (MongoQueryNodeAttribute attributeleft : node.getAttributes()) {
            for (MongoQueryNode noderight : getNonVisitedNodes()) {
                if (!noderight.equals(node)) {
                    for (MongoQueryNodeAttribute attributeright : noderight.getAttributes()) {
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
        for (MongoQueryNode nodeleft : mqns) {
            for (MongoQueryNodeAttribute attributeleft : nodeleft.getAttributes()) {
                for (MongoQueryNode noderight : mqns) {
                    if (!noderight.equals(nodeleft)) {
                        for (MongoQueryNodeAttribute attributeright : noderight.getAttributes()) {
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

    public boolean isNodeRelation(MongoQueryNode node, String attr) {
        if (getNodeRelationList().contains(attr)) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isAttrRelation(MongoQueryNode node, String attr) {
        if (getAttrRelationList(node).contains(attr)) {
            return true;
        } else {
            return false;
        }
    }

    public MongoQueryNode matchNode(StatementPattern sp) {
        for (MongoQueryNode mqn : mqns) {
            if (mqn.getNodeName().equals(sp.getVarList().get(0).getName())) {
                return mqn;
            }
        }
        return null;
    }

    public List<MongoQueryNode> getNodes() {
        return mqns;
    }

    public TupleExpr getExpr() {
        return expr;
    }

    public SPARQLTable getJoinNodesTable(MongoQueryNode node1, MongoQueryNode node2, DBCursor cursor) {
        SPARQLTable result = new SPARQLTable();
        Iterator<DBObject> it = cursor.iterator();
        while (it.hasNext()) {
            SPARQLTable partition = new SPARQLTable();
            DBObject bObject = it.next();
            if (!node1.getTranslatableAttributes().isEmpty()) {
                for (MongoQueryNodeAttribute el : node1.getTranslatableAttributes()) {
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
    }

    public void reduceData(SPARQLTable partition, MongoQueryNodeAttribute el, MongoQueryNode node2) {
        for (SPARQLRow r : node2.getData().getTableList()){
            
        }
    }

    public SPARQLTable getTranslatableNodeTable(MongoQueryNode node, DBCursor cursor) {
        SPARQLTable result = new SPARQLTable();
        Iterator<DBObject> it = cursor.iterator();
        while (it.hasNext()) {
            SPARQLTable partition = new SPARQLTable();
            DBObject bObject = it.next();
            if (!node.getTranslatableAttributes().isEmpty()) {
                for (MongoQueryNodeAttribute el : node.getTranslatableAttributes()) {
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
    }

    public SPARQLTable getNonTranslatableNodeTable(MongoQueryNode node, DBCursor cursor) {
        SPARQLTable result = new SPARQLTable();
        Iterator<DBObject> it = cursor.iterator();
        while (it.hasNext()) {
            SPARQLTable partition = new SPARQLTable();
            DBObject bObject = it.next();
            if (!node.getNonTranslatableAttributes().isEmpty()) {
                for (MongoQueryNodeAttribute el : node.getNonTranslatableAttributes()) {
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
    }

    public void constructJoin(MongoQueryNodeAttribute el, SPARQLTable partition, DBObject bObject) {
        if (el.getSubjectVal() == null) {
            partition.addData(el.getSubject().getName(), (String) bObject.get("_id"));
        }
        if (el.getPredicateVal() == null) {
            ((BasicDBObject) bObject).keySet().forEach((key) -> {
                partition.addData(el.getPredicate().getName(), decodeShort(key));
            });
        }

        if (el.getPredicateVal() != null && el.getObjectVal() == null) {
            BasicDBList bList = (BasicDBList) bObject.get(replaceToShort(el.getPredicateVal()));
            Iterator<Object> itList = bList.iterator();
            while (itList.hasNext()) {
                BasicDBObject bdbo = (BasicDBObject) itList.next();
                String data = bdbo.getString("rdf:resource"); //= .getString("rdf:resource");
                if (data == null) {
                    data = bdbo.getString("textContent");
                }
                partition.addData(el.getObject().getName(), data);
            }
        }

        if (el.getPredicateVal() == null && el.getObjectVal() == null) {
            for (String key : bObject.keySet()) {
                BasicDBList bList = (BasicDBList) bObject.get(replaceToShort(key));
                Iterator<Object> itList = bList.iterator();
                while (itList.hasNext()) {
                    BasicDBObject bdbo = (BasicDBObject) itList.next();
                    String data = bdbo.getString("rdf:resource"); //= .getString("rdf:resource");
                    if (data == null) {
                        data = bdbo.getString("textContent");
                    }
                    partition.addData(el.getObject().getName(), data);
                }
            }
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

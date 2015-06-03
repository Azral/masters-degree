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
import extended.model.OWLOntologyMongoBDStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.Var;

/**
 *
 * @author Арнольд
 */
public class MongoExecuter {
    
    private MongoSPARQLQuery query;
    private OWLOntologyMongoBDStore bDStore;
    
    public MongoExecuter(MongoSPARQLQuery query, OWLOntologyMongoBDStore bDStore) {
        this.query = query;
        this.bDStore = bDStore;
    }
    
    public void execute() {
        
        if (query.getNodes().size() == 1) {
            MongoQueryNode node = query.getNodes().get(0);
            executeNode(node);
            System.out.println(node.getData());
        } else {
            setFirstNode();
            MongoQueryNode node = query.getNodes().get(0);

            executeNode(node);
            query.clearData();
            System.out.println(query.getData());

        }

    }
    
    public void setFirstNode() {
        //query.g
        long minCount = Long.MAX_VALUE;
        MongoQueryNode minNode = null;
        for (MongoQueryNode n : query.getNodes()) {
            DBCursor cursor = null;
            
            BasicDBList l = translateNode(n);
            BasicDBObject bdbo = (BasicDBObject) l.get(0);

            n.setCount(bDStore.countIndividuals(bdbo));
            
        }
        Collections.sort(query.getNodes(), (n1, n2)
                -> Long.compare(n1.getCount(), n2.getCount())
        );

    }
    
    public void executeNode(MongoQueryNode node) {
        List<MongoQueryNode> parentNodes;
        if (node.getData() != null) {
            return;
        }
        node.setVisited();
        parentNodes = query.getParentNodes(node);
        
        SPARQLTable translatableNodeResult;
        SPARQLTable resultTable = null;
        //List<String> relationList;
        if (node.getCount() < 100 || !query.isSimple()) {
            DBCursor cursor = null;
            
            BasicDBList l = translateNode(node);
            BasicDBObject bdbo = (BasicDBObject) l.get(0);
            System.out.println(bdbo);
            cursor = bDStore.findIndividualFields(bdbo, (BasicDBObject) l.get(1));

            translatableNodeResult = query.getTranslatableNodeTable(node, cursor);
            node.setData(translatableNodeResult);
        } 
        if (!query.isSimple()) {
            for (MongoQueryNode n : parentNodes) {
                executeNode(n);
            }
        }
        for (MongoQueryNode n : parentNodes) {
            
            node.setData(joinTranlateNode(node, n));
            node.setCount(node.getData().getTable().size());
            query.setData(node.getData());
        }

        for (MongoQueryNode n : query.getChildNodes(node)) {
            executeNode(n);
            
        }

    }
    
    public SPARQLTable joinTablesMultipleRelations(SPARQLTable leftTable, SPARQLTable rightTable, List<String> joinKeys) {
        SPARQLTable lTable = new SPARQLTable();
        boolean joinable = true;
        //int count = 0;
        if (leftTable.getTable().size() < rightTable.getTable().size()) {
            for (SPARQLRow leftRow : leftTable.getTableList()) {
                for (SPARQLRow rightRow : rightTable.getListByRelations(leftRow.getRelations(joinKeys))) {
                    lTable.addRow(new SPARQLRow(leftRow, rightRow));
                }
            }
        } else {
            for (SPARQLRow rightRow : rightTable.getTableList()) {
                for (SPARQLRow leftRow : leftTable.getListByRelations(rightRow.getRelations(joinKeys))) {
                    lTable.addRow(new SPARQLRow(rightRow, leftRow));
                }
            }
        }

        //System.out.println(lTable.getTable().size());
        return lTable;
    }
    
    public SPARQLTable joinTables(SPARQLTable leftTable, SPARQLTable rightTable, String joinKey) {
        SPARQLTable lTable = new SPARQLTable();
        for (SPARQLRow leftRow : leftTable.getTableList()) {
            for (SPARQLRow rightRow : rightTable.getListByData(joinKey, leftRow.getRow().get(joinKey))) {
                lTable.addRow(new SPARQLRow(leftRow, rightRow));
            }
        }
        return lTable;
    }
    
    public SPARQLTable joinTableAndRow(SPARQLTable leftTable, SPARQLRow rightRow, List<String> joinKeys) {
        SPARQLTable lTable = new SPARQLTable();
        for (SPARQLRow leftRow : leftTable.getTableList()) {
            if (rightRow.getRow().containsKey(joinKeys.get(0)) && leftRow.getRow().containsKey(joinKeys.get(0))) {
                lTable.addRow(new SPARQLRow(leftRow, rightRow));
            }
        }
        return lTable;
    }
    
    public SPARQLTable leftJoinTableAndRow(SPARQLTable leftTable, SPARQLRow rightRow, String joinKey) {
        SPARQLTable lTable = new SPARQLTable();
        for (SPARQLRow leftRow : leftTable.getTableList()) {
            if (rightRow.getRow().containsKey(joinKey) && leftRow.getRow().containsKey(joinKey)) {
                lTable.addRow(new SPARQLRow(leftRow, rightRow));
            } else if (!rightRow.getRow().containsKey(joinKey) && leftRow.getRow().containsKey(joinKey)) {
                lTable.addRow(new SPARQLRow(leftRow, rightRow.getColumns()));
            }
        }
        return lTable;
    }
    
    public SPARQLTable leftJoinTables(SPARQLTable leftTable, SPARQLTable rightTable, String leftJoinKey) {
        SPARQLTable lTable = new SPARQLTable();
        boolean exist;
        for (SPARQLRow leftRow : leftTable.getTableList()) {
            exist = false;
            for (SPARQLRow rightRow : rightTable.getListByData(leftJoinKey, leftRow.getRow().get(leftJoinKey))) {
                lTable.addRow(new SPARQLRow(leftRow, rightRow));
                exist = true;
            }
            if (!exist) {
                lTable.addRow(new SPARQLRow(leftRow, rightTable.getColumns()));
            }
        }
        return lTable;
    }

    //public 
    public BasicDBList translateNode(MongoQueryNode node) {
        node.prepareTranslate();
        BasicDBList l = new BasicDBList();
        BasicDBObject bObject = new BasicDBObject();
        BasicDBObject fields = new BasicDBObject();
        l.add(bObject);
        l.add(fields);
        
        for (MongoQueryNodeAttribute attribute : node.getAttributes()) {
            if (attribute.getExprType().equals("Join")) {
                if (attribute.getSubject().getValue() != null) {
                    bObject.append("_id", attribute.getSubject().getValue().stringValue());
                }
                BasicDBList bList;
                
                if (attribute.getPredicate().getValue() != null && attribute.getObject().getValue() != null) {
                    if (bObject.containsField(replaceToShort(attribute.getPredicate().getValue().stringValue()))) {
                        BasicDBObject bdbo = ((BasicDBObject) bObject.get(replaceToShort(attribute.getPredicate().getValue().stringValue())));
                        if (bdbo.get("$exists") != null) {
                            //bObject.remove(replaceToShort(attribute.getPredicate().getValue().stringValue()));
                            bList = new BasicDBList();
                        } else {
                            bList = (BasicDBList) ((BasicDBObject) bObject.get(replaceToShort(attribute.getPredicate().getValue().stringValue()))).get("$all");
                        }
                    } else {
                        bList = new BasicDBList();
                    }
                    BasicDBObject bObject1;
                    if (attribute.getObject().getValue() instanceof URIImpl) {
                        bObject1 = new BasicDBObject().append("source", replaceToShort(attribute.getPredicate().getValue().stringValue())).append("rdf:resource", attribute.getObject().getValue().stringValue());
                    } else {
                        bObject1 = new BasicDBObject().append("source", replaceToShort(attribute.getPredicate().getValue().stringValue())).append("textContent", attribute.getObject().getValue().stringValue());
                    }
                    bList.add(bObject1);
                    bObject.append(replaceToShort(attribute.getPredicate().getValue().stringValue()), new BasicDBObject().append("$all", bList));
                }
                if (attribute.getPredicate().getValue() != null && attribute.getObject().getValue() == null) {
                    //BasicDBObject bObject1 = new BasicDBObject().append("exist", "true");
                    //bList.add(bObject1);
                    fields.append(replaceToShort(attribute.getPredicate().getValue().stringValue()), 1);
                    bObject.append(replaceToShort(attribute.getPredicate().getValue().stringValue()), new BasicDBObject().append("$exists", "true"));
                }
            }
        }
        return l;
    }
    
    public SPARQLTable joinTranlateNode(MongoQueryNode node1, MongoQueryNode node2) {
        SPARQLTable result = new SPARQLTable();
        
        MongoQueryNode tempNode;
        if (node2.getData() == null) {
            tempNode = node1;
            node1 = node2;
            node2 = tempNode;
        }
        
        SPARQLTable node2Data = node2.getData();
        if (node1.getData() != null && node2.getData() != null) {
            return joinTablesMultipleRelations(node1.getData(), node2.getData(), node1.getData().getRelations(node2.getData()));
        }
        node1.prepareTranslate();
        for (SPARQLRow r : node2Data.getTableList()) {
            DBCursor cursor;
            SPARQLTable nodeResult;
            BasicDBObject bObject = new BasicDBObject();
            for (MongoQueryNodeAttribute attribute : node1.getAttributes()) {
                if (attribute.getExprType().equals("Join")) {
                    if (attribute.getSubject().getValue() != null) {
                        bObject.append("_id", attribute.getSubject().getValue().stringValue());
                    } else if (r.getRow().containsKey(attribute.getSubject().getName())) {
                        bObject.append("_id", r.getRow().get(attribute.getSubject().getName()));
                    }
                    BasicDBList bList;
                    
                    if (attribute.getPredicate().getValue() != null && attribute.getObject().getValue() != null) {
                        if (bObject.containsField(replaceToShort(attribute.getPredicate().getValue().stringValue()))) {
                            BasicDBObject bdbo = ((BasicDBObject) bObject.get(replaceToShort(attribute.getPredicate().getValue().stringValue())));
                            if (bdbo.get("$exists") != null) {
                                //bObject.remove(replaceToShort(attribute.getPredicate().getValue().stringValue()));
                                bList = new BasicDBList();
                            } else {
                                bList = (BasicDBList) ((BasicDBObject) bObject.get(replaceToShort(attribute.getPredicate().getValue().stringValue()))).get("$all");
                            }
                        } else {
                            bList = new BasicDBList();
                        }
                        BasicDBObject bObject1;
                        if (attribute.getObject().getValue() instanceof URIImpl) {
                            bObject1 = new BasicDBObject().append("source", replaceToShort(attribute.getPredicate().getValue().stringValue())).append("rdf:resource", attribute.getObject().getValue().stringValue());
                        } else {
                            bObject1 = new BasicDBObject().append("source", replaceToShort(attribute.getPredicate().getValue().stringValue())).append("textContent", attribute.getObject().getValue().stringValue());
                        }
                        bList.add(bObject1);
                        bObject.append(replaceToShort(attribute.getPredicate().getValue().stringValue()), new BasicDBObject().append("$all", bList));
                    } else if (attribute.getPredicate().getValue() != null && r.getRow().containsKey(attribute.getObject().getName())) {
                        Var attr = node2.getAttributeByName(attribute.getObject().getName());
                        if (bObject.containsField(replaceToShort(attribute.getPredicate().getValue().stringValue()))) {
                            BasicDBObject bdbo = ((BasicDBObject) bObject.get(replaceToShort(attribute.getPredicate().getValue().stringValue())));
                            if (bdbo.get("$exists") != null) {
                                //bObject.remove(replaceToShort(attribute.getPredicate().getValue().stringValue()));
                                bList = new BasicDBList();
                            } else {
                                bList = (BasicDBList) ((BasicDBObject) bObject.get(replaceToShort(attribute.getPredicate().getValue().stringValue()))).get("$all");
                            }
                        } else {
                            bList = new BasicDBList();
                        }
                        BasicDBObject bObject1;
                        bObject1 = new BasicDBObject().append("source", replaceToShort(attribute.getPredicate().getValue().stringValue())).append("rdf:resource", r.getRow().get(attr.getName()));
                        bList.add(bObject1);
                        bObject.append(replaceToShort(attribute.getPredicate().getValue().stringValue()), new BasicDBObject().append("$all", bList));
                    }
                    if (attribute.getPredicate().getValue() != null && attribute.getObject().getValue() == null) {
                        //BasicDBObject bObject1 = new BasicDBObject().append("exist", "true");
                        //bList.add(bObject1);
                        if (!bObject.containsField(replaceToShort(attribute.getPredicate().getValue().stringValue()))) {
                            bObject.append(replaceToShort(attribute.getPredicate().getValue().stringValue()), new BasicDBObject().append("$exists", "true"));
                        }
                    }
                }
            }
            cursor = bDStore.findIndividual(bObject);
            System.out.println(bObject);
            //System.out.println(cursor.count());
            
            nodeResult = query.getTranslatableNodeTable(node1, cursor);
            
            for (SPARQLRow r2 : nodeResult.getTableList()) {
                if (r2.containsRow(r) || r.containsRow(r2)) {
                    result.addRow(new SPARQLRow(r, r2));
                }
            }
            
        }
        
        return result;
    }
    
    public List<DBObject> getCursorList(DBCursor cursor) {
        List<DBObject> resultList = new ArrayList<>();
        return resultList;
    }
    
    private String replaceToShort(String value) {
        String newString = value;
        for (String key : query.getPrefixes().keySet()) {
            if (newString.contains(query.getPrefixes().get(key))) {
                newString = newString.replace(query.getPrefixes().get(key), key + ":");
            }
        }
        return newString;
    }
}

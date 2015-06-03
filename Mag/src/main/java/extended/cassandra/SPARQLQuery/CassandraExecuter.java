/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra.SPARQLQuery;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import extended.apibindings.SPARQLRow;
import extended.apibindings.SPARQLTable;
import extended.cassandra.CassandraQueryResult;
import extended.model.OWLOntologyCassandraStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.Var;

/**
 *
 * @author Арнольд
 */
public class CassandraExecuter {

    private CassandraSPARQLQuery query;
    private OWLOntologyCassandraStore bDStore;

    public CassandraExecuter(CassandraSPARQLQuery query, OWLOntologyCassandraStore bDStore) {
        this.query = query;
        this.bDStore = bDStore;
    }

    public void execute() {

        setFirstNode();
        //for (CassandraQueryNode n : query.getNodes()) {

        executeNode(query.getNodes().get(0));
        // }
        System.out.println(query.getData());

    }

    public void setFirstNode() {
        for (CassandraQueryNode n : query.getNodes()) {

            String query = countTranslateNode(n);
            //System.out.println(query);
            n.setCount(bDStore.countIndividuals(query));

        }
        Collections.sort(query.getNodes(), (n1, n2)
                -> Long.compare(n1.getCount(), n2.getCount())
        );
    }

    public void executeNode(CassandraQueryNode node) {
        List<CassandraQueryNode> relatedNodes;
        //if (node.getData() != null) {
        //     return;
        // }

        relatedNodes = query.getNonVisitedRelatedNodes(node);

        SPARQLTable translatableNodeResult;
        SPARQLTable resultTable = null;
        //List<String> relationList;
        if (node.getData() == null && !node.isVisited()) {
            List<CassandraQueryResult> cursor;
            String l = translateNode(node);
            cursor = bDStore.findNode(l);

            translatableNodeResult = query.getTranslatableNodeTable(node, cursor);
            node.setData(translatableNodeResult);

        }

        for (CassandraQueryNode n : relatedNodes) {
            if (((node.getCount() > 100 && query.getCount() > 100) || !query.isSimple()) && n.getData() == null) {
                List<CassandraQueryResult> cursor2;
                String l2 = translateNode(n);
                cursor2 = bDStore.findNode(l2);

                translatableNodeResult = query.getTranslatableNodeTable(n, cursor2);
                n.setData(translatableNodeResult);
            }

            if (!n.isVisited()) {
                System.out.println(node.getAttributes().get(0).getPattern());
                System.out.println(node.getAttributes().get(0).getExprType());
                System.out.println(node.getCount());
                System.out.println("with");
                System.out.println(n.getAttributes().get(0).getExprType());
                System.out.println(n.getAttributes().get(0).getPattern());
                System.out.println(n.getCount());

                System.out.println();

                if (query.getData() == null) {
                    query.setData(node.getData());
                    query.setCount(node.getCount());
                    node.setVisited();
                }
                if (query.getNonVisitedParentNodes(node).contains(n) && n.getAttributes().get(0).getExprType().equals("LeftJoin")) {
                    continue;
                }

                if (!node.getAttributes().get(0).getExprType().equals("LeftJoin")) {
                    query.setData(joinTranlateNode(n, query.getData()));
                    query.setCount(query.getData().getTable().size());
                } else {
                    query.setData(joinTranlateNode(node, n.getData()));
                    query.setCount(query.getData().getTable().size());
                }
                n.setVisited();
                if (!query.getNonVisitedRelatedNodes(n).isEmpty()) {
                    executeNode(n);
                }
            }
        }

        for (CassandraQueryNode n : query.getNodes()) {
            if (n.getData() == null) {
                //System.out.println(n.getNodeName());
            }
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

    public SPARQLTable leftJoinTablesMultipleRelations(SPARQLTable leftTable, SPARQLTable rightTable, List<String> joinKeys) {
        SPARQLTable lTable = new SPARQLTable();
        boolean exist = false;

        for (SPARQLRow rightRow : rightTable.getTableList()) {
            exist = false;
            for (SPARQLRow leftRow : leftTable.getListByRelations(rightRow.getRelations(joinKeys))) {
                lTable.addRow(new SPARQLRow(rightRow, leftRow));
                exist = true;
            }
            if (exist == false) {
                lTable.addRow(new SPARQLRow(rightRow, leftTable.getColumns()));
            }
        }
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

    public String translateNode(CassandraQueryNode node) {
        String l = "select * ";
        // String cols = "";
        String tableName;
        String whereClause = "";
        CassandraQueryNodeAttribute attr = node.getAttributes().get(0);
        if (attr.isTranslatable()) {
            tableName = replaceToShort(attr.getPredicateVal()).replaceAll(":", "_");
            if (attr.getObjectVal() != null) {

                if (attr.getObject().getValue() instanceof URIImpl) {
                    tableName += "_rs";

                    whereClause += "\"rdf:resource\" = '" + attr.getObjectVal() + "'";
                } else {
                    tableName += "_txt";

                    whereClause += "\"textContent\" = '" + attr.getObjectVal() + "'";
                }

            } else {
                l = "TwoTableSearch" + l;
            }
            if (attr.getSubjectVal() != null) {

                if (whereClause.isEmpty()) {
                    whereClause = "\"parentAbout\" = '" + attr.getSubjectVal() + "'";
                } else {
                    whereClause += " and " + "\"parentAbout\" = '" + attr.getSubjectVal() + "'";
                }
            }
            if (!whereClause.isEmpty()) {
                l = l + " from \"" + tableName + "\" where " + whereClause;
            } else {
                l = l + " from \"" + tableName + "\"";
            }
        } else {
            return "fullSearch";
        }
        return l;
    }

    public String countTranslateNode(CassandraQueryNode node) {
        String l = "select count(*) ";
        String tableName;
        String whereClause = "";
        CassandraQueryNodeAttribute attr = node.getAttributes().get(0);
        if (attr.isTranslatable()) {
            tableName = replaceToShort(attr.getPredicateVal()).replaceAll(":", "_");
            if (attr.getObjectVal() != null) {
                if (attr.getObject().getValue() instanceof URIImpl) {
                    tableName += "_rs";
                    whereClause += "\"rdf:resource\" = '" + attr.getObjectVal() + "'";
                } else {
                    tableName += "_txt";
                    whereClause += "\"textContent\" = '" + attr.getObjectVal() + "'";
                }

            } else {
                l = "TwoTableSearch" + l;
            }
            if (attr.getSubjectVal() != null) {
                if (whereClause.isEmpty()) {
                    whereClause = "\"parentAbout\" = '" + attr.getSubjectVal() + "'";
                } else {
                    whereClause += " and " + "\"parentAbout\" = '" + attr.getSubjectVal() + "'";
                }
            }
            if (!whereClause.isEmpty()) {
                l = l + "from \"" + tableName + "\" where " + whereClause;
            } else {
                l = l + "from \"" + tableName + "\"";
            }
        } else {
            return "fullSearch";
        }
        return l;
    }

    public SPARQLTable joinTranlateNode(CassandraQueryNode node1, SPARQLTable node2) {
        SPARQLTable result = new SPARQLTable();

        CassandraQueryNode tempNode;

        SPARQLTable node2Data = node2;
        if (node1.getData() != null && node2 != null) {
            if (node1.getAttributes().get(0).getExprType().equals("Join")) {
                return joinTablesMultipleRelations(node1.getData(), node2, node1.getData().getRelations(node2));
            } else if (node1.getAttributes().get(0).getExprType().equals("LeftJoin")) {
                return leftJoinTablesMultipleRelations(node1.getData(), node2, node1.getData().getRelations(node2));
            }
        }
        node1.prepareTranslate();

        for (SPARQLRow r : node2Data.getTableList()) {
            List<CassandraQueryResult> cursor;
            SPARQLTable nodeResult;
            String l = "select * from ";
            String whereClause = "";
            for (CassandraQueryNodeAttribute attribute : node1.getAttributes()) {
                // if (attribute.getExprType().equals("Join")) {
                String tablePx = "";
                if (attribute.getSubject().getValue() != null) {
                    whereClause += "\"parentAbout\" = '" + attribute.getSubject().getValue().stringValue() + "'";
                } else if (r.getRow().containsKey(attribute.getSubject().getName())) {
                    whereClause += "\"parentAbout\" = '" + r.getRow().get(attribute.getSubject().getName()) + "'";
                }

                if (attribute.getObject().getValue() != null) {
                    String column;
                    if (attribute.getObject().getValue() instanceof URIImpl) {
                        column = "rdf:resource";
                        tablePx = "_rs";
                    } else {
                        column = "textContent";
                        tablePx = "_txt";
                    }
                    if (whereClause.isEmpty()) {
                        whereClause += "\"" + column + "\" = '" + attribute.getObject().getValue().stringValue() + "'";
                    } else {
                        whereClause += " and \"" + column + "\" = '" + attribute.getObject().getValue().stringValue() + "'";
                    }

                } else if (r.getRow().containsKey(attribute.getObject().getName())) {
                    //Var attr = node2.getAttributeByName(attribute.getObject().getName());
                    tablePx = "_rs";
                    if (whereClause.isEmpty()) {
                        whereClause += "\"rdf:resource\" = '" + r.getRow().get(attribute.getObject().getName()) + "'";
                    } else {
                        whereClause += " and \"rdf:resource\" = '" + r.getRow().get(attribute.getObject().getName()) + "'";
                    }
                } else {
                    l = "TwoTableSearch" + l;
                }

                if (attribute.getPredicateVal() != null) {
                    l += "\"" + replaceToShort(attribute.getPredicateVal()).replaceAll(":", "_") + tablePx + "\"";
                } else {
                    l = "fullSearch" + l;
                }

                if (!whereClause.isEmpty()) {
                    l += " where " + whereClause;
                }

            }
            System.out.println(l);
            cursor = bDStore.findNode(l);

            nodeResult = query.getTranslatableNodeTable(node1, cursor);

            for (SPARQLRow r2 : nodeResult.getTableList()) {
                if (node1.getAttributes().get(0).getExprType().equals("Join")) {
                    boolean exist = true;
                    for (String col : r2.getRelations(r)) {
                        if (!r.getRow().get(col).equals(r2.getRow().get(col))) {
                            exist = false;
                        }
                    }

                    if (exist) {
                        result.addRow(new SPARQLRow(r, r2));
                    }
                } else {
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

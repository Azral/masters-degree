/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra.SPARQLQuery;

import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 *
 * @author Арнольд
 */
public class CassandraSPARQLVisitor extends QueryModelVisitorBase<RuntimeException> {

    private CassandraSPARQLQuery mquery = new CassandraSPARQLQuery();

    public CassandraSPARQLVisitor() {
    }

    @Override
    public void meet(LeftJoin node) throws RuntimeException {
        TupleExpr leftArg = node.getLeftArg();
        TupleExpr rightArg = node.getRightArg();
        if (leftArg instanceof Join) {
            meet((Join) leftArg);
        }
        if (rightArg instanceof Join) {
            meet((Join) rightArg);
        }

        if (leftArg instanceof LeftJoin) {
            meet((LeftJoin) leftArg);
        }
        if (rightArg instanceof LeftJoin) {
            meet((LeftJoin) rightArg);
        }

        if (leftArg instanceof StatementPattern) {
            mquery.addStatement((StatementPattern) leftArg, "LeftJoin");
        }
        if (rightArg instanceof StatementPattern) {
            mquery.addStatement((StatementPattern) rightArg, "LeftJoin");
        }
    }

    @Override
    public void meet(Join node) throws RuntimeException {

        //node.getLeftArg().getSignature()
        //node.v
        TupleExpr leftArg = node.getLeftArg();
        TupleExpr rightArg = node.getRightArg();
        if (leftArg instanceof Join) {
            meet((Join) leftArg);
        }
        if (rightArg instanceof Join) {
            meet((Join) rightArg);
        }

        if (leftArg instanceof LeftJoin) {
            meet((LeftJoin) leftArg);
        }
        if (rightArg instanceof LeftJoin) {
            meet((LeftJoin) rightArg);
        }

        if (leftArg instanceof StatementPattern) {
             mquery.addStatement((StatementPattern) leftArg, "Join");
        }
        if (rightArg instanceof StatementPattern) {
            mquery.addStatement((StatementPattern) rightArg, "Join");
        }
        //System.out.println(leftArg.getClass());
        // System.out.println(leftArg.getParentNode());
        //super.meet(node); //To change body of generated methods, choose Tools | Templates.
    }

    public CassandraSPARQLQuery getMongoSPARQLQuery() {
        return mquery;
    }

    @Override
    public void meet(StatementPattern node) throws RuntimeException {
        //node.
        super.meet(node); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void meetNode(QueryModelNode node) {
        super.meetNode(node);
    }
}

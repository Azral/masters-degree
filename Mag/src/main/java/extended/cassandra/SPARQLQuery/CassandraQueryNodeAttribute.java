/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra.SPARQLQuery;

import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 *
 * @author Арнольд
 */
public class CassandraQueryNodeAttribute {

    private StatementPattern pattern;
    private String exprType;
    private boolean translatable;
    

    public CassandraQueryNodeAttribute(StatementPattern attribute, String exprType) {
        this.pattern = attribute;
        this.exprType = exprType;
        translatable = true;
    }

    public StatementPattern getPattern() {
        return pattern;
    }

    public String getExprType() {
        return exprType;
    }

    public String getAttributeName() {
        return pattern.getVarList().get(0).getName();
    }

    public boolean isTranslatable() {
        return translatable;
    }

    public void setTranslatable(boolean translatable) {
        this.translatable = translatable;
    }

    public Var getSubject() {
        return pattern.getSubjectVar();
    }

    public String getSubjectVal() {
        if (pattern.getSubjectVar().getValue() == null) {
            return null;
        }
        return pattern.getSubjectVar().getValue().stringValue();
    }

    public Var getPredicate() {
        return pattern.getPredicateVar();
    }

    public String getPredicateVal() {
        if (pattern.getPredicateVar().getValue() == null) {
            return null;
        }
        return pattern.getPredicateVar().getValue().stringValue();
    }

    public Var getObject() {
        return pattern.getObjectVar();
    }

    public String getObjectVal() {
        if (pattern.getObjectVar().getValue() == null) {
            return null;
        }
        return pattern.getObjectVar().getValue().stringValue();
    }

}

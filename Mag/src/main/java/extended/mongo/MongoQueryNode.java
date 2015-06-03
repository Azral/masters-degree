/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.mongo;

import extended.apibindings.SPARQLTable;
import java.util.ArrayList;
import java.util.List;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

/**
 *
 * @author Арнольд
 */
public class MongoQueryNode {

    private List<MongoQueryNodeAttribute> attributes = new ArrayList<>();
    private SPARQLTable data;
    private boolean isVisited = false;
    private long count;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
    public MongoQueryNode(StatementPattern node, String exprType) {
        attributes.add(new MongoQueryNodeAttribute(node, exprType));
    }

    public String getNodeName() {
        if (!attributes.isEmpty()) {
            return attributes.get(0).getAttributeName();
        }
        return null;
    }

    public SPARQLTable getData() {
        return data;
    }

    public boolean isVisited() {
        return isVisited;
    }

    public void setVisited() {
        this.isVisited = true;
    }

    public void setData(SPARQLTable data) {
        this.data = data;
    }

    public void addAttribute(MongoQueryNodeAttribute mqna) {
        attributes.add(mqna);
    }

    public List<MongoQueryNodeAttribute> getAttributes() {
        return attributes;
    }

    public Var getAttributeByName(String name) {
        for (MongoQueryNodeAttribute attribute : attributes) {
            if (attribute.getSubject().getName().equals(name)) {
                return attribute.getSubject();
            } else if (attribute.getObject().getName().equals(name)) {
                return attribute.getObject();
            } else {
                return attribute.getPredicate();
            }
        }
        return null;
    }

    public void prepareTranslate() {
        for (MongoQueryNodeAttribute attribute : attributes) {
            if (attribute.getSubjectVal() == null && attribute.getPredicateVal() == null) {
                attribute.setTranslatable(false);
            }
        }
    }

    public boolean isTranslatable() {
        for (MongoQueryNodeAttribute el : attributes) {
            if (!el.isTranslatable()) {
                return false;
            }
        }
        return true;
    }

    public List<String> getRelations(MongoQueryNode n) {
        List<String> result = new ArrayList<>();
        for (MongoQueryNodeAttribute attributeleft : getAttributes()) {
            for (MongoQueryNodeAttribute attributeright : n.getAttributes()) {
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

                }
            }
        }
        return result;
    }

    public List<MongoQueryNodeAttribute> getTranslatableAttributes() {
        List<MongoQueryNodeAttribute> ls = new ArrayList<>();
        for (MongoQueryNodeAttribute el : attributes) {
            if (el.isTranslatable()) {
                ls.add(el);
            }
        }
        return ls;
    }

    public List<MongoQueryNodeAttribute> getNonTranslatableAttributes() {
        List<MongoQueryNodeAttribute> ls = new ArrayList<>();
        for (MongoQueryNodeAttribute el : attributes) {
            if (!el.isTranslatable()) {
                ls.add(el);
            }
        }
        return ls;
    }

}

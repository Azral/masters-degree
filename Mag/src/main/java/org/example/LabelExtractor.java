/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.example;

import org.semanticweb.owlapi.model.OWLAnnotation;
import org.semanticweb.owlapi.model.OWLAnnotationObjectVisitorEx;
import org.semanticweb.owlapi.model.OWLLiteral;
import org.semanticweb.owlapi.util.OWLObjectVisitorExAdapter;

/**
 *
 * @author Арнольд
 */
class LabelExtractor extends OWLObjectVisitorExAdapter<String>
        implements OWLAnnotationObjectVisitorEx<String> {

    @Override
    public  String visit(OWLAnnotation annotation) {
        if (annotation.getProperty().isLabel()) {
            OWLLiteral c = (OWLLiteral) annotation.getValue();
            return c.getLiteral();
        }
        return null;
    }
}

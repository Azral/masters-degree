/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.rdf.rdfxml;

import extended.mongo.RDFXMLMongoDBRenderer;
import extended.util.AbstractOWLOntologyNoSQLStorer;
import java.io.IOException;
import java.io.Writer;
import java.util.Set;
import org.coode.owlapi.rdf.rdfxml.RDFXMLRenderer;

import org.coode.xml.IllegalElementNameException;
import org.semanticweb.owlapi.io.RDFXMLOntologyFormat;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyFormat;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

/**
 * @author Matthew Horridge, The University Of Manchester, Bio-Health
 * Informatics Group, Date: 03-Jan-2007
 */
public class RDFXMLOntologyMongoDBStorer extends AbstractOWLOntologyNoSQLStorer {

    private static final long serialVersionUID = 30406L;

    @Override
    public boolean canStoreOntology(OWLOntologyFormat ontologyFormat) {
        return ontologyFormat instanceof RDFXMLOntologyFormat;
    }

    @Override
    protected void storeOntology(OWLOntologyManager manager,
            OWLOntology ontology, Writer writer, OWLOntologyFormat format)
            throws OWLOntologyStorageException {
        storeOntology(ontology, writer, format);
    }

    @Override
    protected void storeOntology(OWLOntology ontology, Writer writer,
            OWLOntologyFormat format) throws OWLOntologyStorageException {
        try {
            RDFXMLMongoDBRenderer renderer = new RDFXMLMongoDBRenderer(ontology, writer,
                    format);
            Set<OWLEntity> entities = renderer.getUnserialisableEntities();
            if (!entities.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (OWLEntity entity : entities) {
                    sb.append(entity.toStringID());
                    sb.append("\n");
                }
                throw new OWLOntologyStorageException(sb.toString().trim(),
                        new IllegalElementNameException(sb.toString().trim()));
            }
            renderer.render();
        } catch (IOException e) {
            throw new OWLOntologyStorageException(e);
        } catch (IllegalElementNameException e) {
            throw new OWLOntologyStorageException(e);
        }
    }
}

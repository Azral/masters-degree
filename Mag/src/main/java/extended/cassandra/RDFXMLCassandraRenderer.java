/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra;

import extended.impl.OWLOntologyCassandraStoreImpl;
import extended.model.OWLOntologyCassandraStore;
import extended.rdf.rdfxml.RDFRendererNoSQLBase;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.coode.owlapi.rdf.model.RDFLiteralNode;
import org.coode.owlapi.rdf.model.RDFNode;
import org.coode.owlapi.rdf.model.RDFResourceNode;
import org.coode.owlapi.rdf.model.RDFTriple;
import org.coode.owlapi.rdf.rdfxml.RDFXMLNamespaceManager;
import org.semanticweb.owlapi.io.RDFOntologyFormat;
import org.semanticweb.owlapi.io.XMLUtils;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAnnotationProperty;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataProperty;
import org.semanticweb.owlapi.model.OWLDatatype;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyFormat;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.util.VersionInfo;
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary;
import static org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDF_DESCRIPTION;

/**
 * @author Matthew Horridge, The University Of Manchester, Bio-Health
 * Informatics Group, Date: 06-Dec-2006
 */
public class RDFXMLCassandraRenderer extends RDFRendererNoSQLBase {

    private RDFXMLCassandraWriter writer;
    private Set<RDFResourceNode> pending = new HashSet<RDFResourceNode>();
    private RDFXMLNamespaceManager qnameManager;
    private OWLOntologyFormat format;
    private OWLOntologyCassandraStore cassandraStore;

    /**
     * @param manager manager
     * @param ontology ontology
     * @param w writer
     */
    @SuppressWarnings("unused")
    @Deprecated
    public RDFXMLCassandraRenderer(OWLOntologyManager manager, OWLOntology ontology,
            Writer w) {
        this(ontology, w, ontology.getOWLOntologyManager().getOntologyFormat(
                ontology));
    }

    /**
     * @param ontology ontology
     * @param w writer
     */
    public RDFXMLCassandraRenderer(OWLOntology ontology, Writer w) {
        this(ontology, w, ontology.getOWLOntologyManager().getOntologyFormat(
                ontology));
    }

    /**
     * @param manager manager
     * @param ontology ontology
     * @param w writer
     * @param format format
     */
    @SuppressWarnings("unused")
    @Deprecated
    public RDFXMLCassandraRenderer(OWLOntologyManager manager, OWLOntology ontology,
            Writer w, OWLOntologyFormat format) {
        this(ontology, w, format);
    }

    /**
     * @param ontology ontology
     * @param w writer
     * @param format format
     */
    public RDFXMLCassandraRenderer(OWLOntology ontology, Writer w,
            OWLOntologyFormat format) {
        super(ontology, format);
        this.format = format;
        try {
            cassandraStore = new OWLOntologyCassandraStoreImpl("test");
        } catch (Exception e) {
            e.printStackTrace();
        }
        qnameManager = new RDFXMLNamespaceManager(ontology, format);
        String defaultNamespace = qnameManager.getDefaultNamespace();
        String base;
        if (defaultNamespace.endsWith("#")) {
            base = defaultNamespace.substring(0, defaultNamespace.length() - 1);
        } else {
            base = defaultNamespace;
        }
        writer = new RDFXMLCassandraWriter(new CassandraWriter(cassandraStore, qnameManager, base));
        prettyPrintedTypes = new HashSet<IRI>();
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_CLASS.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_OBJECT_PROPERTY.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_DATA_PROPERTY.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_ANNOTATION_PROPERTY
                .getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_RESTRICTION.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_THING.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_NOTHING.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_ONTOLOGY.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_ANNOTATION_PROPERTY
                .getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_NAMED_INDIVIDUAL.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.RDFS_DATATYPE.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_AXIOM.getIRI());
        prettyPrintedTypes.add(OWLRDFVocabulary.OWL_ANNOTATION.getIRI());
    }

    /**
     * @return unserializable entities
     */
    public Set<OWLEntity> getUnserialisableEntities() {
        return qnameManager.getEntitiesWithInvalidQNames();
    }

    @Override
    protected void beginDocument() throws IOException {
        writer.startDocument();
    }

    @Override
    protected void endDocument() throws IOException {
        writer.endDocument();
        writer.writeComment(VersionInfo.getVersionInfo()
                .getGeneratedByMessage());
        if (format instanceof RDFOntologyFormat
                && !((RDFOntologyFormat) format).isAddMissingTypes()) {
            // missing type declarations could have been omitted, adding a
            // comment to document it
            writer.writeComment("Warning: type declarations were not added automatically.");
        }
    }

    @Override
    protected void writeIndividualComments(OWLNamedIndividual ind)
            throws IOException {
        writer.writeComment(XMLUtils.escapeXML(ind.getIRI().toString()));
    }

    @Override
    protected void writeClassComment(OWLClass cls) throws IOException {
        writer.writeComment(XMLUtils.escapeXML(cls.getIRI().toString()));
    }

    @Override
    protected void writeDataPropertyComment(OWLDataProperty prop)
            throws IOException {
        writer.writeComment(XMLUtils.escapeXML(prop.getIRI().toString()));
    }

    @Override
    protected void writeObjectPropertyComment(OWLObjectProperty prop)
            throws IOException {
        writer.writeComment(XMLUtils.escapeXML(prop.getIRI().toString()));
    }

    @Override
    protected void writeAnnotationPropertyComment(OWLAnnotationProperty prop)
            throws IOException {
        writer.writeComment(XMLUtils.escapeXML(prop.getIRI().toString()));
    }

    @Override
    protected void writeDatatypeComment(OWLDatatype datatype)
            throws IOException {
        writer.writeComment(XMLUtils.escapeXML(datatype.getIRI().toString()));
    }

    @Override
    protected void writeBanner(String name) throws IOException {
        writer.writeComment("\n///////////////////////////////////////////////////////////////////////////////////////\n"
                + "//\n"
                + "// "
                + name
                + "\n"
                + "//\n"
                + "///////////////////////////////////////////////////////////////////////////////////////\n");
    }

    @Override
    public void render(RDFResourceNode node) throws IOException {
        if (pending.contains(node)) {
            return;
        }
        pending.add(node);
        RDFTriple candidatePrettyPrintTypeTriple = null;
        Collection<RDFTriple> triples = getGraph().getTriplesForSubject(node,
                true);
        for (RDFTriple triple : triples) {
            IRI propertyIRI = triple.getProperty().getIRI();
            if (propertyIRI.equals(OWLRDFVocabulary.RDF_TYPE.getIRI())
                    && !triple.getObject().isAnonymous()) {
                if (OWLRDFVocabulary.BUILT_IN_VOCABULARY_IRIS.contains(triple
                        .getObject().getIRI())) {
                    if (prettyPrintedTypes
                            .contains(triple.getObject().getIRI())) {
                        candidatePrettyPrintTypeTriple = triple;
                    }
                }
            }
        }
        if (candidatePrettyPrintTypeTriple == null) {
            writer.writeStartElement(RDF_DESCRIPTION.getIRI());
        } else {
            writer.writeStartElement(candidatePrettyPrintTypeTriple.getObject()
                    .getIRI());
        }
        if (!node.isAnonymous()) {
            writer.writeAboutAttribute(node.getIRI());
        }
        // XXX this call looks like it should be made, but only when the node id
        // is necessary
        // else {
        // writer.writeNodeIDAttribute(node);
        // }
        for (RDFTriple triple : triples) {
            if (candidatePrettyPrintTypeTriple != null
                    && candidatePrettyPrintTypeTriple.equals(triple)) {
                continue;
            }
            writer.writeStartElement(triple.getProperty().getIRI());
            RDFNode objectNode = triple.getObject();
            if (!objectNode.isLiteral()) {
                RDFResourceNode objectRes = (RDFResourceNode) objectNode;
                if (objectRes.isAnonymous()) {
                    // Special rendering for lists
                    if (isObjectList(objectRes)) {
                        writer.writeParseTypeAttribute();
                        List<RDFNode> list = new ArrayList<RDFNode>();
                        toJavaList(objectRes, list);
                        for (RDFNode n : list) {
                            if (n.isAnonymous()) {
                                render((RDFResourceNode) n);
                            } else {
                                if (n.isLiteral()) {
                                    RDFLiteralNode litNode = (RDFLiteralNode) n;
                                    writer.writeStartElement(OWLRDFVocabulary.RDFS_LITERAL
                                            .getIRI());
                                    if (litNode.getDatatype() != null) {
                                        writer.writeDatatypeAttribute(litNode
                                                .getDatatype());
                                    } else if (litNode.getLang() != null) {
                                        writer.writeLangAttribute(litNode
                                                .getLang());
                                    }
                                    writer.writeTextContent(litNode
                                            .getLiteral());
                                    writer.writeEndElement();
                                } else {
                                    writer.writeStartElement(RDF_DESCRIPTION
                                            .getIRI());
                                    writer.writeAboutAttribute(n.getIRI());
                                    writer.writeEndElement();
                                }
                            }
                        }
                    } else {
                        render(objectRes);
                    }
                } else {
                    writer.writeResourceAttribute(objectRes.getIRI());
                }
            } else {
                RDFLiteralNode rdfLiteralNode = (RDFLiteralNode) objectNode;
                if (rdfLiteralNode.getDatatype() != null) {
                    writer.writeDatatypeAttribute(rdfLiteralNode.getDatatype());
                } else if (rdfLiteralNode.getLang() != null) {
                    writer.writeLangAttribute(rdfLiteralNode.getLang());
                }
                writer.writeTextContent(rdfLiteralNode.getLiteral());
            }
            writer.writeEndElement();
        }
        writer.writeEndElement();
        pending.remove(node);
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra;

import com.mongodb.DBObject;
import java.io.IOException;

import org.coode.owlapi.rdf.model.RDFResourceNode;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLObject;
import org.semanticweb.owlapi.vocab.Namespaces;

/**
 * @author Matthew Horridge, The University Of Manchester, Bio-Health Informatics
 *         Group, Date: 06-Dec-2006
 */
public class RDFXMLCassandraWriter {

    private static final IRI RDF_RDF = IRI.create(
            Namespaces.RDF.getPrefixIRI(), "RDF");
    private static final IRI RDF_RESOURCE = IRI.create(
            Namespaces.RDF.getPrefixIRI(), "resource");
    private static final String XML_LANG = "xml:lang";
    private static final IRI RDF_NODEID = IRI.create(
            Namespaces.RDF.getPrefixIRI(), "nodeID");
    private static final IRI RDF_ABOUT = IRI.create(
            Namespaces.RDF.getPrefixIRI(), "about");
    private static final IRI RDF_DATATYPE = IRI.create(
            Namespaces.RDF.getPrefixIRI(), "datatype");
    private static final IRI PARSETYPE_IRI = IRI.create(
            Namespaces.RDF.getPrefixIRI(), "parseType");
    private CassandraWriter writer;
    private DBObject bObject;

    protected RDFXMLCassandraWriter(CassandraWriter writer) {
        this.writer = writer;
    }

    /**
     * @param elementName
     *        elementName
     * @throws IOException
     *         io exception
     */
    public void writeStartElement(IRI elementName) throws IOException {
        // Sort out with namespace
        writer.writeStartElement(elementName);
    }

    /**
     * @throws IOException
     *         io exception
     */
    public void writeParseTypeAttribute() throws IOException {
        writer.writeAttribute(PARSETYPE_IRI, "Collection");
    }

    /**
     * @param datatypeIRI
     *        datatypeIRI
     * @throws IOException
     *         io exception
     */
    public void writeDatatypeAttribute(IRI datatypeIRI) throws IOException {
        writer.writeAttribute(RDF_DATATYPE, datatypeIRI.toString());
    }

    /**
     * @param text
     *        text
     * @throws IOException
     *         io exception
     */
    public void writeTextContent(String text) throws IOException {
        writer.writeTextContent(text);
    }

    /**
     * @param lang
     *        lang
     * @throws IOException
     *         io exception
     */
    public void writeLangAttribute(String lang) throws IOException {
        writer.writeAttribute(XML_LANG, lang);
    }

    /**
     * @throws IOException
     *         io exception
     */
    public void writeEndElement() throws IOException {
        writer.writeEndElement();
    }

    /**
     * @param value
     *        value
     * @throws IOException
     *         io exception
     */
    public void writeAboutAttribute(IRI value) throws IOException {
        writeAttribute(RDF_ABOUT, value);
    }

    /**
     * @param node
     *        node
     * @throws IOException
     *         io exception
     */
    public void writeNodeIDAttribute(RDFResourceNode node) throws IOException {
        writer.writeAttribute(RDF_NODEID, node.toString());
    }

    private void writeAttribute(IRI attributeName, IRI value)
            throws IOException {
        writer.writeAttribute(attributeName, value.toString());
    }

    /**
     * @param owlObject
     *        owlObject
     */
    @SuppressWarnings("unused")
    public void writeOWLObject(OWLObject owlObject) {}

    /**
     * @param value
     *        value
     * @throws IOException
     *         io exception
     */
    public void writeResourceAttribute(IRI value) throws IOException {
        writeAttribute(RDF_RESOURCE, value);
    }

    /**
     * @throws IOException
     *         io exception
     */
    public void startDocument() throws IOException {
        writer.startDocument(RDF_RDF);
    }

    /**
     * @throws IOException
     *         io exception
     */
    public void endDocument() throws IOException {
        writer.endDocument();
    }

    /**
     * @param comment
     *        comment
     * @throws IOException
     *         io exception
     */
    public void writeComment(String comment) throws IOException {
        writer.writeComment(comment);
    }
}
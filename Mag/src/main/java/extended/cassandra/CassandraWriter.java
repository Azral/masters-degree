/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import extended.model.OWLOntologyCassandraStore;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import org.coode.xml.IllegalElementNameException;
import org.coode.xml.XMLWriter;
import org.coode.xml.XMLWriterNamespaceManager;
import org.coode.xml.XMLWriterPreferences;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.vocab.OWL2Datatype;

/**
 *
 * @author Арнольд
 */
public class CassandraWriter implements XMLWriter {

    private Stack<XMLElement> elementStack;
    protected OWLOntologyCassandraStore writer;
    private String encoding = "";
    private String xmlBase;
    private URI xmlBaseURI;
    private XMLWriterNamespaceManager xmlWriterNamespaceManager;
    private Map<String, String> entities;
    private boolean preambleWritten;
    private static final String PERCENT_ENTITY = "&#37;";

    /**
     * @param writer writer
     * @param xmlWriterNamespaceManager xmlWriterNamespaceManager
     * @param xmlBase xmlBase
     */
    public CassandraWriter(OWLOntologyCassandraStore writer,
            XMLWriterNamespaceManager xmlWriterNamespaceManager, String xmlBase) {
        this.writer = writer;
        this.xmlWriterNamespaceManager = xmlWriterNamespaceManager;
        this.xmlBase = xmlBase;

        xmlBaseURI = URI.create(xmlBase);
        // no need to set it to UTF-8: it's supposed to be the default encoding
        // for XML.
        // Must be set correctly for the Writer anyway, or bugs will ensue.
        // this.encoding = "UTF-8";
        elementStack = new Stack<>();
        setupEntities();
    }

    private void setupEntities() {
        List<String> namespaces = new ArrayList<>(
                xmlWriterNamespaceManager.getNamespaces());
        Collections.sort(namespaces, new StringLengthOnlyComparator());
        entities = new LinkedHashMap<>();
        for (String curNamespace : namespaces) {
            String curPrefix = "";
            if (xmlWriterNamespaceManager.getDefaultNamespace().equals(
                    curNamespace)) {
                curPrefix = xmlWriterNamespaceManager.getDefaultPrefix();
            } else {
                curPrefix = xmlWriterNamespaceManager
                        .getPrefixForNamespace(curNamespace);
            }
            if (curPrefix.length() > 0) {
                entities.put(curNamespace, "&" + curPrefix + ";");
            }
        }
    }

    protected String swapForEntity(String value) {
        for (String curEntity : entities.keySet()) {
            String entityVal = entities.get(curEntity);
            if (value.length() > curEntity.length()) {
                String repVal = value.replace(curEntity, entityVal);
                if (repVal.length() < value.length()) {
                    return repVal;
                }
            }
        }
        return value;
    }

    /**
     * @return default namespace
     */
    public String getDefaultNamespace() {
        return xmlWriterNamespaceManager.getDefaultNamespace();
    }

    public String getXMLBase() {
        return xmlBase;
    }

    /**
     * @return xml base
     */
    public URI getXMLBaseAsURI() {
        return xmlBaseURI;
    }

    public XMLWriterNamespaceManager getNamespacePrefixes() {
        return xmlWriterNamespaceManager;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    private boolean isValidQName(String name) {
        if (name == null) {
            return false;
        }
        int colonIndex = name.indexOf(":");
        boolean valid = false;
        if (colonIndex == -1) {
            valid = OWL2Datatype.XSD_NCNAME.getPattern().matcher(name)
                    .matches();
        } else {
            valid = OWL2Datatype.XSD_NCNAME.getPattern()
                    .matcher(name.substring(0, colonIndex - 1)).matches();
            if (valid) {
                valid = OWL2Datatype.XSD_NAME.getPattern()
                        .matcher(name.substring(colonIndex + 1)).matches();
            }
        }
        return valid;
    }

    public void setWrapAttributes(boolean b) {
    }

    public void writeStartElement(String name) throws IOException {
        String qName = xmlWriterNamespaceManager.getQName(name);
        if (qName == null || qName.equals(name)) {
            if (!isValidQName(name)) {
                // Could not generate a valid QName, therefore, we cannot
                // write valid XML - just throw an exception!
                throw new IllegalElementNameException(name);
            }
        }
        XMLElement element = new XMLElement(qName, elementStack.size());
        if (!elementStack.isEmpty()) {
            XMLElement topElement = elementStack.peek();
            if (topElement != null) {
                topElement.writeElementStart(false);
            }
        }
        elementStack.push(element);
    }

    public void writeStartElement(IRI name) throws IOException {
        String qName = xmlWriterNamespaceManager.getQName(name);
        if (qName.length() == name.length()) {
            // Could not generate a valid QName, therefore, we cannot
            // write valid XML - just throw an exception!
            throw new IllegalElementNameException(name.toString());
        }
        XMLElement element = new XMLElement(qName, elementStack.size());
        if (!elementStack.isEmpty()) {
            XMLElement topElement = elementStack.peek();
            if (topElement != null) {
                topElement.writeElementStart(false);
            }
        }
        elementStack.push(element);
    }

    public void writeEndElement() throws IOException {
        // Pop the element off the stack and write it out
        if (!elementStack.isEmpty()) {
            XMLElement element = elementStack.pop();

            element.writeElementEnd();
            if (!elementStack.isEmpty()) {
                XMLElement pred = elementStack.peek();
                pred.addCell(element.getTableName(), "TableReference_" + element.getTableName());
                if (!pred.data.containsKey("rdf:about")) {
                    pred.data.put("rdf:about", writer.getSurrogateKey());
                }
                element.addCell("parentAbout", pred.data.get("rdf:about"));
                //pred.appendBasicDBObject(element.getBasicDBObject()); //TableReference_
            }
            writer.createTable(element.data.keySet(), element.getTableName());
            writer.insertRow(element.data,element.getTableName());
        }
    }

    public void writeAttribute(String attr, String val) {
        XMLElement element = elementStack.peek();
        String qName = xmlWriterNamespaceManager.getQName(attr);
        if (qName != null) {
            element.setAttribute(qName, val);
        }
    }

    public void writeAttribute(IRI attr, String val) {
        XMLElement element = elementStack.peek();
        String qName = xmlWriterNamespaceManager.getQName(attr);
        if (qName != null) {
            element.setAttribute(qName, val);
        }
    }

    public void writeTextContent(String text) {
        XMLElement element = elementStack.peek();
        element.setText(text);
    }

    public void writeComment(String commentText) throws IOException {
        XMLElement element = new XMLElement(null, elementStack.size());
        element.setText("<!-- " + commentText.replace("--", "&#45;&#45;")
                + " -->");
        if (!elementStack.isEmpty()) {
            XMLElement topElement = elementStack.peek();
            if (topElement != null) {
                topElement.writeElementStart(false);
            }
        }
        if (preambleWritten) {
            element.writeElementStart(true);
        } else {
            elementStack.push(element);
        }
    }

    private void writeEntities(IRI rootName) throws IOException {
        String qName = xmlWriterNamespaceManager.getQName(rootName);
        if (qName == null) {
            throw new IOException("Cannot create valid XML: qname for "
                    + rootName + " is null");
        }
    }

    public void startDocument(String rootElementName) throws IOException {
        startDocument(IRI.create(rootElementName));
    }

    public void startDocument(IRI rootElement) throws IOException {
        String encodingString = "";
        if (encoding.length() > 0) {
            encodingString = " encoding=\"" + encoding + "\"";
        }

        
        if (XMLWriterPreferences.getInstance().isUseNamespaceEntities()) {
            writeEntities(rootElement);
        }
        preambleWritten = true;
        while (!elementStack.isEmpty()) {
            elementStack.pop().writeElementStart(true);
        }
        writeStartElement(rootElement);
        writeAttribute("xmlVersion", "<?xml version=\"1.0\"" + encodingString + "?>\n");
        writeAttribute("xmlns", xmlWriterNamespaceManager.getDefaultNamespace());
        if (xmlBase.length() != 0) {
            writeAttribute("xml:base", xmlBase);
        }
        for (String curPrefix : xmlWriterNamespaceManager.getPrefixes()) {
            if (curPrefix.length() > 0) {
                writeAttribute("xmlns:" + curPrefix,
                        xmlWriterNamespaceManager
                        .getNamespaceForPrefix(curPrefix));
            }
        }
    }

    public void endDocument() throws IOException {
        while (!elementStack.isEmpty()) {
            writeEndElement();
        }
        //writer.createNameSpace(bObject);
        writer.flush();
    }

    private static final class StringLengthOnlyComparator implements
            Comparator<String>, Serializable {

        private static final long serialVersionUID = 30406L;

        public StringLengthOnlyComparator() {
        }

        @Override
        public int compare(String o1, String o2) {
            // Shortest string first
            return o1.length() - o2.length();
        }
    }

    /**
     * xml element
     */
    public class XMLElement {

        private String name;
        private Map<String, String> attributes;
        String textContent;
        private boolean startWritten;
        private HashMap<String, String> data;
        private String tableName;

        public XMLElement(String name) {
            this(name, 0);
        }

        public XMLElement(String name, int indentation) {
            this.name = name;
            data = new HashMap<>();
            attributes = new LinkedHashMap<>();
            textContent = null;
            startWritten = false;
        }

        public boolean isNamedIndividual() {
            if (data.get("source").contains("NamedIndividual")) {
                return true;
            } else {
                return false;
            }
        }

        public String getTableName() {
            return tableName;
        }

        public void addCell(String col, String dt) {
            data.put(col, dt);
        }

        public HashMap<String, String> getData() {
            return data;
        }

        public void setAttribute(String attribute, String value) {
            attributes.put(attribute, value);
        }

        public void setText(String content) {
            textContent = content;
        }

        public void writeElementStart(boolean close) throws IOException {
            if (!startWritten) {
                startWritten = true;
                if (name != null) {
                    tableName = name;
                    writeAttributes();
                    if (textContent != null) {
                        writeTextContent();
                    }
                    if (close) {
                        if (textContent != null) {
                            writeElementEnd();
                        }
                    }
                }
            }
        }

        public void writeElementEnd() throws IOException {
            if (name != null) {
                if (!startWritten) {
                    writeElementStart(true);
                }
            }
        }

        private void writeAttribute(String attr, String val) throws IOException {
            data.put(attr, val);
        }

        private void writeAttributes() throws IOException {
            for (String attr : attributes.keySet()) {
                String val = attributes.get(attr);
                writeAttribute(attr, val);
            }
        }

        private void writeTextContent() throws IOException {
            if (textContent != null) {
                writeAttribute("textContent", textContent);
            }
        }
    }
}

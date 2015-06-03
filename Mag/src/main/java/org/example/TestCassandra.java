/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.example;

/**
 *
 * @author Арнольд
 */
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

//import javax.annotation.Nonnull;
//import org.semanticweb.HermiT.Reasoner;
import extended.apibindings.OWLManagerNoSQL;
import extended.impl.OWLOntologyCassandraStoreImpl;
import extended.impl.OWLOntologyMongoBDStoreImpl;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLException;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import extended.model.OWLOntologyManagerNoSQL;
import org.semanticweb.owlapi.reasoner.BufferingMode;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.Node;
import org.semanticweb.owlapi.reasoner.NodeSet;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;
import org.semanticweb.owlapi.reasoner.structural.StructuralReasoner;
import org.semanticweb.owlapi.reasoner.structural.StructuralReasonerFactory;

/**
 * @author Matthew Horridge, The University Of Manchester, Bio-Health
 * Informatics Group
 * @since 2.0.0
 */
@SuppressWarnings({"javadoc", "unused", "null"})
public class TestCassandra {

//    private static LabelExtractor le = new LabelExtractor();
    //@Nonnull
    public static void shouldLoad() throws Exception {
        // return;
        // Get hold of an ontology manager
        //OWLOntologyCassandraStoreImpl owlocsi = new OWLOntologyCassandraStoreImpl("test");
        OWLOntologyCassandraStoreImpl impl = new OWLOntologyCassandraStoreImpl("test");
        impl.drop();
        OWLOntologyManagerNoSQL manager = OWLManagerNoSQL.createOWLOntologyManager();
        // Remove the ontology so that we can load a local copy.
        OWLOntology localTest = loadTest(manager);
//        Reasoner hermit = new Reasoner(localTest);
         //manager.saveOntology(localTest);
        manager.saveOntologyCassandra(localTest);
        // manager.saveOntology(localTest);
    }

    static OWLOntology loadTest(OWLOntologyManagerNoSQL manager)
            throws OWLOntologyCreationException, IOException {
        // List<String> list = Files.readAllLines(new File("D:\\OWLDownload\\University0_2.owl").toPath(), Charset.defaultCharset());
        String test = readFile("D:\\OWLDownload\\University0_3_short.owl");
        File file2 = new File("D:\\OWLDownload\\University0_3_short.owl");
        // for (String line : list) {
        //     test += line;
        // }
        // Let's load an ontology from the web
        // IRI iri = IRI
        // .create("http://owl.cs.manchester.ac.uk/co-ode-files/ontologies/pizza.owl");
        // OWLOntology pizzaOntology = manager
        // .loadOntologyFromOntologyDocument(iri);
        // in this test, we load from a string instead
        //manager.s
        return manager
                .loadOntologyFromOntologyDocument(file2/*new StringDocumentSource(
                 test)*/);
    }

    private static String readFile(String pathname) throws IOException {

        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(file);
        String lineSeparator = System.getProperty("line.separator");

        try {
            while (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine() + lineSeparator);
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

    public static void main(String[] args) {
        try {
            //OWLOntologyMongoBDStoreImpl bDStore = new OWLOntologyMongoBDStoreImpl("test");
            // bDStore.testData();
            shouldLoad();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

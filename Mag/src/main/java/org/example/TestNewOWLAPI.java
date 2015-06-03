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
public class TestNewOWLAPI {

//    private static LabelExtractor le = new LabelExtractor();
    //@Nonnull
    private static final String KOALA = "<?xml version=\"1.0\"?>\n"
            + "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:owl=\"http://www.w3.org/2002/07/owl#\" xmlns=\"http://protege.stanford.edu/plugins/owl/owl-library/koala.owl#\" xml:base=\"http://protege.stanford.edu/plugins/owl/owl-library/koala.owl\">\n"
            + "  <owl:Ontology rdf:about=\"\"/>\n"
            + "  <owl:Class rdf:ID=\"Female\"><owl:equivalentClass><owl:Restriction><owl:onProperty><owl:FunctionalProperty rdf:about=\"#hasGender\"/></owl:onProperty><owl:hasValue><Gender rdf:ID=\"female\"/></owl:hasValue></owl:Restriction></owl:equivalentClass></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Marsupials\"><owl:disjointWith><owl:Class rdf:about=\"#Person\"/></owl:disjointWith><rdfs:subClassOf><owl:Class rdf:about=\"#Animal\"/></rdfs:subClassOf></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Student\"><owl:equivalentClass><owl:Class><owl:intersectionOf rdf:parseType=\"Collection\"><owl:Class rdf:about=\"#Person\"/><owl:Restriction><owl:onProperty><owl:FunctionalProperty rdf:about=\"#isHardWorking\"/></owl:onProperty><owl:hasValue rdf:datatype=\"http://www.w3.org/2001/XMLSchema#boolean\">true</owl:hasValue></owl:Restriction><owl:Restriction><owl:someValuesFrom><owl:Class rdf:about=\"#University\"/></owl:someValuesFrom><owl:onProperty><owl:ObjectProperty rdf:about=\"#hasHabitat\"/></owl:onProperty></owl:Restriction></owl:intersectionOf></owl:Class></owl:equivalentClass></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"KoalaWithPhD\"><owl:versionInfo>1.2</owl:versionInfo><owl:equivalentClass><owl:Class><owl:intersectionOf rdf:parseType=\"Collection\"><owl:Restriction><owl:hasValue><Degree rdf:ID=\"PhD\"/></owl:hasValue><owl:onProperty><owl:ObjectProperty rdf:about=\"#hasDegree\"/></owl:onProperty></owl:Restriction><owl:Class rdf:about=\"#Koala\"/></owl:intersectionOf></owl:Class></owl:equivalentClass></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"University\"><rdfs:subClassOf><owl:Class rdf:ID=\"Habitat\"/></rdfs:subClassOf></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Koala\"><rdfs:subClassOf><owl:Restriction><owl:hasValue rdf:datatype=\"http://www.w3.org/2001/XMLSchema#boolean\">false</owl:hasValue><owl:onProperty><owl:FunctionalProperty rdf:about=\"#isHardWorking\"/></owl:onProperty></owl:Restriction></rdfs:subClassOf><rdfs:subClassOf><owl:Restriction><owl:someValuesFrom><owl:Class rdf:about=\"#DryEucalyptForest\"/></owl:someValuesFrom><owl:onProperty><owl:ObjectProperty rdf:about=\"#hasHabitat\"/></owl:onProperty></owl:Restriction></rdfs:subClassOf><rdfs:subClassOf rdf:resource=\"#Marsupials\"/></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Animal\"><rdfs:seeAlso>Male</rdfs:seeAlso><rdfs:subClassOf><owl:Restriction><owl:onProperty><owl:ObjectProperty rdf:about=\"#hasHabitat\"/></owl:onProperty><owl:minCardinality rdf:datatype=\"http://www.w3.org/2001/XMLSchema#int\">1</owl:minCardinality></owl:Restriction></rdfs:subClassOf><rdfs:subClassOf><owl:Restriction><owl:cardinality rdf:datatype=\"http://www.w3.org/2001/XMLSchema#int\">1</owl:cardinality><owl:onProperty><owl:FunctionalProperty rdf:about=\"#hasGender\"/></owl:onProperty></owl:Restriction></rdfs:subClassOf><owl:versionInfo>1.1</owl:versionInfo></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Forest\"><rdfs:subClassOf rdf:resource=\"#Habitat\"/></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Rainforest\"><rdfs:subClassOf rdf:resource=\"#Forest\"/></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"GraduateStudent\"><rdfs:subClassOf><owl:Restriction><owl:onProperty><owl:ObjectProperty rdf:about=\"#hasDegree\"/></owl:onProperty><owl:someValuesFrom><owl:Class><owl:oneOf rdf:parseType=\"Collection\"><Degree rdf:ID=\"BA\"/><Degree rdf:ID=\"BS\"/></owl:oneOf></owl:Class></owl:someValuesFrom></owl:Restriction></rdfs:subClassOf><rdfs:subClassOf rdf:resource=\"#Student\"/></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Parent\"><owl:equivalentClass><owl:Class><owl:intersectionOf rdf:parseType=\"Collection\"><owl:Class rdf:about=\"#Animal\"/><owl:Restriction><owl:onProperty><owl:ObjectProperty rdf:about=\"#hasChildren\"/></owl:onProperty><owl:minCardinality rdf:datatype=\"http://www.w3.org/2001/XMLSchema#int\">1</owl:minCardinality></owl:Restriction></owl:intersectionOf></owl:Class></owl:equivalentClass><rdfs:subClassOf rdf:resource=\"#Animal\"/></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"DryEucalyptForest\"><rdfs:subClassOf rdf:resource=\"#Forest\"/></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Quokka\"><rdfs:subClassOf><owl:Restriction><owl:hasValue rdf:datatype=\"http://www.w3.org/2001/XMLSchema#boolean\">true</owl:hasValue><owl:onProperty><owl:FunctionalProperty rdf:about=\"#isHardWorking\"/></owl:onProperty></owl:Restriction></rdfs:subClassOf><rdfs:subClassOf rdf:resource=\"#Marsupials\"/></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"TasmanianDevil\"><rdfs:subClassOf rdf:resource=\"#Marsupials\"/></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"MaleStudentWith3Daughters\"><owl:equivalentClass><owl:Class><owl:intersectionOf rdf:parseType=\"Collection\"><owl:Class rdf:about=\"#Student\"/><owl:Restriction><owl:onProperty><owl:FunctionalProperty rdf:about=\"#hasGender\"/></owl:onProperty><owl:hasValue><Gender rdf:ID=\"male\"/></owl:hasValue></owl:Restriction><owl:Restriction><owl:onProperty><owl:ObjectProperty rdf:about=\"#hasChildren\"/></owl:onProperty><owl:cardinality rdf:datatype=\"http://www.w3.org/2001/XMLSchema#int\">3</owl:cardinality></owl:Restriction><owl:Restriction><owl:allValuesFrom rdf:resource=\"#Female\"/><owl:onProperty><owl:ObjectProperty rdf:about=\"#hasChildren\"/></owl:onProperty></owl:Restriction></owl:intersectionOf></owl:Class></owl:equivalentClass></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Degree\"/>\n  <owl:Class rdf:ID=\"Gender\"/>\n"
            + "  <owl:Class rdf:ID=\"Male\"><owl:equivalentClass><owl:Restriction><owl:hasValue rdf:resource=\"#male\"/><owl:onProperty><owl:FunctionalProperty rdf:about=\"#hasGender\"/></owl:onProperty></owl:Restriction></owl:equivalentClass></owl:Class>\n"
            + "  <owl:Class rdf:ID=\"Person\"><rdfs:subClassOf rdf:resource=\"#Animal\"/><owl:disjointWith rdf:resource=\"#Marsupials\"/></owl:Class>\n"
            + "  <owl:ObjectProperty rdf:ID=\"hasHabitat\"><rdfs:range rdf:resource=\"#Habitat\"/><rdfs:domain rdf:resource=\"#Animal\"/></owl:ObjectProperty>\n"
            + "  <owl:ObjectProperty rdf:ID=\"hasDegree\"><rdfs:domain rdf:resource=\"#Person\"/><rdfs:range rdf:resource=\"#Degree\"/></owl:ObjectProperty>\n"
            + "  <owl:ObjectProperty rdf:ID=\"hasChildren\"><rdfs:range rdf:resource=\"#Animal\"/><rdfs:domain rdf:resource=\"#Animal\"/></owl:ObjectProperty>\n"
            + "  <owl:FunctionalProperty rdf:ID=\"hasGender\"><rdfs:range rdf:resource=\"#Gender\"/><rdf:type rdf:resource=\"http://www.w3.org/2002/07/owl#ObjectProperty\"/><rdfs:domain rdf:resource=\"#Animal\"/></owl:FunctionalProperty>\n"
            + "  <owl:FunctionalProperty rdf:ID=\"isHardWorking\"><rdfs:range rdf:resource=\"http://www.w3.org/2001/XMLSchema#boolean\"/><rdfs:domain rdf:resource=\"#Person\"/><rdf:type rdf:resource=\"http://www.w3.org/2002/07/owl#DatatypeProperty\"/></owl:FunctionalProperty>\n"
            + "  <Degree rdf:ID=\"MA\"/>\n</rdf:RDF>";

    /**
     * The examples here show how to load ontologies.
     *
     * @throws Exception exception
     */
    public static void shouldLoad() throws Exception {
        // return;
        // Get hold of an ontology manager
        OWLOntologyManagerNoSQL manager = OWLManagerNoSQL.createOWLOntologyManager();

        // Remove the ontology so that we can load a local copy.
        OWLOntology localTest = loadTest(manager);
//        Reasoner hermit = new Reasoner(localTest);
        manager.saveOntologyMongoDB(localTest);
        manager.saveOntology(localTest);
        return;
        //org.semanticweb.HermiT.model.
        //    boolean consistent2 = hermit.isConsistent();
        //   System.out.println(hermit.isConsistent());
        //OWLDataFactory factory = manager.getOWLDataFactory();
        // We can always obtain the location where an ontology was loaded from;
        // for this test, though, since the ontology was loaded from a string,
        // this does not return a file
        IRI documentIRI = manager.getOntologyDocumentIRI(localTest);
        System.out.println(documentIRI);
        // Remove the ontology again so we can reload it later
        //manager.removeOntology(localTest);
        // In cases where a local copy of one of more ontologies is used, an
        // ontology IRI mapper can be used to provide a redirection mechanism.
        // This means that ontologies can be loaded as if they were located on
        // the web. In this example, we simply redirect the loading from
        // http://owl.cs.manchester.ac.uk/co-ode-files/ontologies/pizza.owl to
        // our local copy
        // above.
        // iri and file here are used as examples
        /*IRI iri = IRI
         .create("http://owl.cs.manchester.ac.uk/co-ode-files/ontologies/pizza.owl");
         File file = folder.newFile();
         manager.getIRIMappers().add(new SimpleIRIMapper(iri, IRI.create(file)));
         // Load the ontology as if we were loading it from the web (from its
         // ontology IRI)
         IRI pizzaOntologyIRI = IRI
         .create("http://owl.cs.manchester.ac.uk/co-ode-files/ontologies/pizza.owl");
         OWLOntology redirectedPizza = manager.loadOntology(pizzaOntologyIRI);
         IRI pizza = manager.getOntologyDocumentIRI(redirectedPizza);*/
        // Note that when imports are loaded an ontology manager will be
        // searched for mappings

        OWLReasonerFactory reasonerFactory2 = new StructuralReasonerFactory();
        OWLReasoner reasoner2 = reasonerFactory2.createReasoner(localTest);
        // Ask the reasoner to do all the necessary work now
        reasoner2.precomputeInferences(InferenceType.DATA_PROPERTY_ASSERTIONS);
        // We can determine if the ontology is actually consistent (in this
        // case, it should be).
        boolean consistent = reasoner2.isConsistent();
        System.out.println(consistent);
        Node<OWLClass> bottomNode = reasoner2.getUnsatisfiableClasses();
        // This node contains owl:Nothing and all the classes that are
        // equivalent to owl:Nothing - i.e. the unsatisfiable classes. We just
        // want to print out the unsatisfiable classes excluding owl:Nothing,
        // and we can used a convenience method on the node to get these
        Set<OWLClass> unsatisfiable = bottomNode.getEntitiesMinusBottom();
        if (!unsatisfiable.isEmpty()) {
            // System.out.println("The following classes are unsatisfiable: ");
            for (OWLClass cls : unsatisfiable) {
                System.out.println("    " + cls);
            }
        } else {
            System.out.println("There are no unsatisfiable classes");
        }

        // OWLDataFactory fac = manager.getOWLDataFactory();
        //OWLClass vegPizza;
        // vegPizza = fac.getOWLClass(IRI
        // .create("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person"));
        // Now use the reasoner to obtain the subclasses of vegetarian. We can
        // ask for the direct subclasses of vegetarian or all of the (proper)
        // subclasses of vegetarian. In this case we just want the direct ones
        // (which we specify by the "true" flag).
        //NodeSet<OWLClass> subClses = reasoner.getSubClasses(vegPizza, true);
        // Set<OWLClass> clses = subClses.getFlattened();
        // System.out.println("Subclasses: ");
        // for (OWLClass cls : clses) {
        //  System.out.println("    " + cls);
        //  }
        OWLDataFactory fac = manager.getOWLDataFactory();

        OWLClass toppingCls = fac.getOWLClass(IRI.create("http://swat.cse.lehigh.edu/onto/univ-bench.owl#ResearchAssistant"));
        Set<OWLEntity> sig = new HashSet<OWLEntity>();
        sig.add(toppingCls);
        // We now add all subclasses (direct and indirect) of the chosen
        // classes. Ideally, it should be done using a DL reasoner, in order to
        // take inferred subclass relations into account. We are using the
        // structural reasoner of the OWL API for simplicity.

        Set<OWLEntity> seedSig = new HashSet<OWLEntity>();
        OWLReasoner reasoner = new StructuralReasoner(localTest,
                new SimpleConfiguration(), BufferingMode.NON_BUFFERING);
        for (OWLClass c : localTest.getClassesInSignature()) {
            if (c.getIRI().getFragment().equals("ResearchAssistant")) {
                NodeSet<OWLNamedIndividual> instances = reasoner.getInstances(c, false);
                System.out.println(c.getIRI());
                for (OWLNamedIndividual i : instances.getFlattened()) {
                    System.out.println(i.getIRI());
                }
            }
        }
        System.out
                .println("Extracting the module for the seed signature consisting of the following entities:");
        for (OWLEntity ent : seedSig) {
            System.out.println("  " + ent);
        }
        System.out.println("Some statistics of the original ontology:");
        System.out.println("  " + localTest.getSignature().size()
                + " entities");
        System.out.println("  " + localTest.getLogicalAxiomCount()
                + " logical axioms");
        System.out.println("  "
                + (localTest.getAxiomCount() - localTest.getLogicalAxiomCount())
                + " other axioms");

        OWLDataFactory df = OWLManagerNoSQL.getOWLDataFactory();

        OWLClass clazz = df.getOWLThing();
        System.out.println("Class : " + clazz);
        OWLReasoner reasoner3 = new StructuralReasoner(localTest,
                new SimpleConfiguration(), BufferingMode.NON_BUFFERING);
        new TestNewOWLAPI().printHierarchy(reasoner3, clazz, 1, new HashSet<OWLClass>(), localTest);
        // NodeSet<OWLClass> subClses = reasoner3.getSubClasses(clazz, true);
        //   Set<OWLClass> clses = subClses.getFlattened();
        //  System.out.println("Subclasses: ");
        // for (OWLClass cls : clses) {
        //  System.out.println("    " + cls);
        //  }

    }

    /*private String labelFor(OWLEntity clazz, OWLOntology o) {

     Set<OWLAnnotation> annotations = clazz.getAnnotations(o);
     for (OWLAnnotation anno : annotations) {
     String result = anno.accept(le);
     if (result != null) {
     return result;
     }
     }
     return clazz.getIRI().toString();
     }*/
    public void printHierarchy(OWLReasoner r, OWLClass clazz,
            int level, Set<OWLClass> visited, OWLOntology localTest) throws OWLException {
//Only print satisfiable classes to skip Nothing
        if (!visited.contains(clazz) && r.isSatisfiable(clazz)) {
            visited.add(clazz);
            for (int i = 0; i < level * 4; i++) {
                System.out.print(" ");
            }
            System.out.println(clazz);

            /*for (OWLAnnotation prop : clazz.getAnnotations(localTest)) {
             for (int i = 0; i < level * 5; i++) {
             System.out.print(" ");
             }
             System.out.println(prop);
             }*/

            /*for (OWLAxiom expr : clazz.get(localTest)) {
             for (int i = 0; i < level * 5; i++) {
             System.out.print(" ");
             }
             System.out.println(expr);
             }*/
// Find the children and recurse
            NodeSet<OWLClass> classes = r.getSubClasses(clazz, true);
            for (OWLClass child : classes.getFlattened()) {
                printHierarchy(r, child, level + 1, visited, localTest);
            }
        }
    }

    /**
     * @param manager manager
     * @return loaded ontology
     * @throws OWLOntologyCreationException if a problem pops up
     */
//    @Nonnull
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

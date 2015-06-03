/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import extended.apibindings.OWLManagerNoSQL;
import extended.cassandra.SPARQLQuery.CassandraQuery;
import extended.impl.OWLOntologyCassandraStoreImpl;
import extended.model.OWLOntologyManagerNoSQL;
import java.io.File;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.semanticweb.owlapi.model.OWLOntology;

/**
 *
 * @author Арнольд
 */
public class TestOWLCassandra {

     static String queryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
            + "SELECT ?X ?Y\n"
            + "WHERE \n"
            + "{?X rdf:type ub:UndergraduateStudent .\n"
            + "  ?Y rdf:type ub:Course .\n"
            + "  ?X ub:takesCourse ?Y .\n"
            + "  <http://www.Department0.University0.edu/AssociateProfessor0>   \n"
            + "  	ub:teacherOf ?Y}";
    static String queryString2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
            + "SELECT ?X ?Y ?Z\n"
            + "WHERE\n"
            + "{?X rdf:type ub:GraduateStudent.\n"
            + "  ?Y rdf:type ub:University.\n"
            + "  ?Z rdf:type ub:Department.\n"
            + "  ?X ub:memberOf ?Z.\n"
            + "  ?Z ub:subOrganizationOf ?Y.\n"
            + "  ?X ub:undergraduateDegreeFrom ?Y}";
    
    public TestOWLCassandra() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test
    public void testLoadCassandra() throws Exception {
        OWLOntologyCassandraStoreImpl bDStore = new OWLOntologyCassandraStoreImpl("test",true);
        //bDStore.drop();
        String resultList = "";
        String resultQuery = "";
        String resultQuery2 = "";
        int step = 5;
        OWLOntologyCassandraStoreImpl.setBase("http://swat.cse.lehigh.edu/onto/univ-bench.owl#");
        //long start = System.currentTimeMillis();
        File folder = new File("D:\\OWLDownload\\uba1.7\\data\\");
        File[] files = folder.listFiles();
        Arrays.sort(files, (f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified())); /* new Comparator<File>() {
         public int compare(File f1, File f2) {
         return Long.compare(f1.lastModified(), f2.lastModified());
         }
         });*/

        int aggrTime = 0;
        int count = 0;
        for (File file : files) {
            OWLOntologyManagerNoSQL manager = OWLManagerNoSQL.createOWLOntologyManager();
            // Remove the ontology so that we can load a local copy.
            String fileName = file.getName();
            Pattern pattern = Pattern.compile(".*sity([0-9]+)_.*");
            Matcher matcher = pattern.matcher(fileName);
            
            int currFileId = 0;
            if (matcher.find()) {
                currFileId = Integer.valueOf(matcher.group(1));
            }
            if (currFileId > 50) {
                break;
            }

            OWLOntology localTest = manager.loadOntologyFromOntologyDocument(file);
//        Reasoner hermit = new Reasoner(localTest);

            long start = System.currentTimeMillis();
            manager.saveOntologyCassandra(localTest);
            long stop = System.currentTimeMillis();
            aggrTime += (stop - start);
            if (currFileId % step == 0 && currFileId != 0) {
                resultList += currFileId + ";" + ((double) aggrTime / 1000.0) + "\r\n";
                count++;
                //System.out.println(count);
                if (count < 4){
                CassandraQuery mq = new CassandraQuery(queryString);
                start = System.currentTimeMillis();
                mq.execute();
                stop = System.currentTimeMillis();
                //System.out.println(count);

                CassandraQuery mq3 = new CassandraQuery(queryString2);
                long start2 = System.currentTimeMillis();
                mq3.execute();
                long stop2 = System.currentTimeMillis();
                //}
                resultQuery += currFileId + ";" + ((double) (stop - start) / 1000.0) + "\r\n"; //System.out.print

                resultQuery2 += currFileId + ";" + ((double) (stop2 - start2) / 1000.0) + "\r\n"; //System.out.print
                //flag = false;
                }
                //System.out.print
            } else {
                count = 0;
            }
            //manager.setBase();
                /*BufferedReader br = new BufferedReader(new FileReader("E:\\uba1.7\\data\\University" + i + "_" + j + ".owl"));
             String line;
             if (resultOnt == null) {
             resultOnt = br.readLine();
             }
             start = false;
             end = false;
             while ((line = br.readLine()) != null) {
             if (first) {
             if (!line.contains("</rdf:RDF>")) {
             resultOnt += line + System.getProperty("line.separator");
             }
             } else {
             if (line.contains("<rdf:RDF")) {
             start = true;
             }
             if (start == true && line.contains("</owl:Ontology>")) {
             end = true;
             }
             if (start == true && end == true) {
             resultOnt += line + System.getProperty("line.separator");
             }
             }
             }
             first = false;*/
        }
        OWLOntologyCassandraStoreImpl.close();
        System.out.println(resultList);
        System.out.println(resultQuery);
        System.out.println(resultQuery2);
        // long stop = System.currentTimeMillis();
        // System.out.println((stop - start));
        //resultOnt += "</rdf:RDF>";
        // OWLOntologyMongoBDStore bDStore = new OWLOntologyMongoBDStoreImpl("test");
        //bDStore.clearDB();
    }
}

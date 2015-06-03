/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.nativerdf.NativeStore;

/**
 *
 * @author Арнольд
 */
public class TestNativeSesame {

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

    public TestNativeSesame() {
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
    public void testNativeSesame() throws Exception {

        String resultList = "";
        String resultQuery = "";
        String resultQuery2 = "";
        String dbSize = "";
        int step = 5;
        File dataDir = new File("D:\\OWLDownload\\test");
        Repository nativeRep = new SailRepository(new NativeStore(dataDir));
        nativeRep.initialize();
        RepositoryConnection con = nativeRep.getConnection();
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

//        Reasoner hermit = new Reasoner(localTest);
            long start = System.currentTimeMillis();
            con.add(file, "file://" + file.getAbsolutePath(), RDFFormat.RDFXML);
            con.commit();
            long stop = System.currentTimeMillis();
            aggrTime += (stop - start);
            System.out.println(file.getAbsolutePath());
            if (currFileId % step == 0 && currFileId != 0) {
                resultList += currFileId + ";" + ((double) aggrTime / 1000.0) + "\r\n";
                //System.out.print
                count++;
                if (count < 2) {
                    con.commit();
                    //File f = new File("D:\\OWLDownload\\test");
                    long size = Files.walk(Paths.get("D:\\OWLDownload\\test")).mapToLong(p -> p.toFile().length()).sum();
                    dbSize += currFileId + ";" + size + "\r\n";
                    // Repository nativeRep2 = new SailRepository(new NativeStore(dataDir));
                    // nativeRep2.initialize();
                    RepositoryConnection con2 = nativeRep.getConnection();
                    TupleQuery tq = con2.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
                    start = System.currentTimeMillis();

                    TupleQueryResult tqr = tq.evaluate();
                    while (tqr.hasNext()) {
                        BindingSet s = tqr.next();
                        System.out.println(s);
                    }
                    stop = System.currentTimeMillis();
                    System.out.println("/////queryString2////");
                    RepositoryConnection con3 = nativeRep.getConnection();
                    TupleQuery tq2 = con3.prepareTupleQuery(QueryLanguage.SPARQL, queryString2);

                    long start2 = System.currentTimeMillis();

                    TupleQueryResult tqr2 = tq2.evaluate();
                    while (tqr2.hasNext()) {
                        BindingSet s = tqr2.next();
                        System.out.println(s);
                    }
                    long stop2 = System.currentTimeMillis();
                    con2.close();
                    con3.close();
                    //nativeRep2.shutDown();
                    resultQuery += currFileId + ";" + ((double) (stop - start) / 1000.0) + "\r\n"; //System.out.print

                    resultQuery2 += currFileId + ";" + ((double) (stop2 - start2) / 1000.0) + "\r\n"; //System.out.print

                }
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
        con.close();
        System.out.println(resultList);
        System.out.println(resultQuery);
        System.out.println(resultQuery2);
        System.out.println(dbSize);
        // long stop = System.currentTimeMillis();
        // System.out.println((stop - start));
        //resultOnt += "</rdf:RDF>";
        // OWLOntologyMongoBDStore bDStore = new OWLOntologyMongoBDStoreImpl("test");
        //bDStore.clearDB();
    }
}

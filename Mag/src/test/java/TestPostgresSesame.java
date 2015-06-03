/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import extended.apibindings.OWLManagerNoSQL;
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
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.nativerdf.NativeStore;
import org.openrdf.sail.rdbms.postgresql.PgSqlStore;
import org.semanticweb.owlapi.model.OWLOntology;

/**
 *
 * @author Арнольд
 */
public class TestPostgresSesame {

    public TestPostgresSesame() {
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
    static String queryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
            + "SELECT ?X ?Y ?Z\n"
            + "WHERE\n"
            + "{?X rdf:type ub:GraduateStudent.\n"
            + "  ?Y rdf:type ub:University.\n"
            + "  ?Z rdf:type ub:Department.\n"
            + "  ?X ub:memberOf ?Z.\n"
            + "  ?Z ub:subOrganizationOf ?Y.\n"
            + "  ?X ub:undergraduateDegreeFrom ?Y}";

    @Test
    public void testPostgresSesame() throws Exception {

        String resultList = "";
        String resultQuery = "";
        int step = 10;
        //File dataDir = new File("D:\\OWLDownload\\test");
        PgSqlStore pgst = new PgSqlStore("sesame_store");
        pgst.setPassword("AA11bb22");
        pgst.setUser("postgres");
        Repository postgreRep = new SailRepository(pgst);
        //postgreRep.
        postgreRep.initialize();
        RepositoryConnection con = postgreRep.getConnection();
        //long start = System.currentTimeMillis();
        File folder = new File("D:\\OWLDownload\\uba1.7\\data\\");
        File[] files = folder.listFiles();
        Arrays.sort(files, (f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified())); /* new Comparator<File>() {
         public int compare(File f1, File f2) {
         return Long.compare(f1.lastModified(), f2.lastModified());
         }
         });*/

        boolean flag = true;
        int aggrTime = 0;
        for (File file : files) {

            // Remove the ontology so that we can load a local copy.
            String fileName = file.getName();
            Pattern pattern = Pattern.compile(".*sity([0-9]+)_.*");
            Matcher matcher = pattern.matcher(fileName);

            int currFileId = 0;
            if (matcher.find()) {
                currFileId = Integer.valueOf(matcher.group(1));
            }
            if (currFileId > 20) {
                break;
            }

//        Reasoner hermit = new Reasoner(localTest);
            long start = System.currentTimeMillis();
            con.add(file, "file://" + file.getAbsolutePath(), RDFFormat.RDFXML);
            con.commit();
            long stop = System.currentTimeMillis();
            aggrTime += (stop - start);
            System.out.println(file.getAbsolutePath());

            if (currFileId % step == 0) {
                resultList += currFileId + ";" + ((double) aggrTime / 1000.0) + "\r\n";
                if (flag) {
                    TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
                    start = System.currentTimeMillis();

                    TupleQueryResult tqr = tq.evaluate();
                    while (tqr.hasNext()) {
                        BindingSet s = tqr.next();
                        System.out.println(s);
                    }
                    stop = System.currentTimeMillis();
                    flag = false;
                    resultQuery += currFileId + ";" + ((double) ( stop - start) / 1000.0) + "\r\n"; //System.out.print
                }

            } else {
                flag = true;
            }
        }
        con.close();
        System.out.println(resultList);
        System.out.println(resultQuery);
    }
}

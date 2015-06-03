/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.example;

import java.io.File;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepository;

import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.sail.nativerdf.NativeStore;

/**
 *
 * @author Арнольд
 */
public class testSPARQL {

    static String queryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
     + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
     + "SELECT ?X ?Y ?Z	\n"
     + "WHERE\n"
     + "{?X rdf:type ?Y .\n"
     + "  ?X ub:takesCourse ?Z."
     + "?X ub:takesCourse\n"
     + "<http://www.Department0.University0.edu/GraduateCourse64>}";

    public static void main(String[] args) {
        try {
            SPARQLParserFactory factory = new SPARQLParserFactory();
            QueryParser parser = factory.getParser();
            ParsedQuery parsedQuery = parser.parseQuery(queryString, null);
            parsedQuery.toString();
            System.out.println(parsedQuery);
            StatementPatternCollector collector = new StatementPatternCollector();
            TupleExpr tupleExpr = parsedQuery.getTupleExpr();
            tupleExpr.visit(collector);
            File dataDir = new File("D:\\OWLDownload\\test");
            long start = System.currentTimeMillis();
            Repository nativeRep = new SailRepository(new NativeStore(dataDir));
            nativeRep.initialize();
            RepositoryConnection con = nativeRep.getConnection();
            TupleQuery tq = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            

            TupleQueryResult tqr = tq.evaluate();
            int count=0;
            while (tqr.hasNext()) {
                BindingSet s = tqr.next();
                System.out.println(s);
                count++;
            }
            long stop = System.currentTimeMillis();
            System.out.println(stop - start);
            System.out.println(count);
            con.close();
            nativeRep.shutDown();
            //ParsedGraphQuery graphQuery = new ParsedGraphQuery(tupleExpr);
            //tupleExpr.
            int a = 1 + 1;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

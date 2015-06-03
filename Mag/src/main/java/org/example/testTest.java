/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.example;

import java.io.File;
import java.util.HashMap;
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
public class testTest {

    private static HashMap<HashMap<String, String>, String> test = new HashMap<>();

    public static void main(String[] args) {
        HashMap<String, String> tt = new HashMap<>();
        tt.put("X", "http://www.Department0.University0.edu/AssociateProfessor0");
        test.put(tt, "AssociateProfessor0");
        HashMap<String, String> tt2 = new HashMap<>();
        tt2.put("X", "http://www.Department0.University0.edu/AssociateProfessor0");
        System.out.println(test.get(tt2));
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;

/**
 *
 * @author Арнольд
 */
public class CassandraQuery {

    public CassandraQuery(String query) throws MalformedQueryException {
        this(query, null);
    }

    public CassandraQuery(String query, String base) throws MalformedQueryException {
        SPARQLParserFactory factory = new SPARQLParserFactory();
        QueryParser parser = factory.getParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, base);
    }

}

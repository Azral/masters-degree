/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra.SPARQLQuery;

import extended.impl.OWLOntologyCassandraStoreImpl;
import extended.model.OWLOntologyCassandraStore;
import java.net.UnknownHostException;
import java.util.Map;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.BindingAssigner;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.openrdf.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.openrdf.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.PrefixDeclProcessor;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;

/**
 *
 * @author �������
 */
public class CassandraQuery {

    private static OWLOntologyCassandraStore mongoBDStore;
    private CassandraSPARQLVisitor msparql;
    private TupleExpr expr;
    private CassandraSPARQLQuery mQuery;

    static {
        try {
            mongoBDStore = new OWLOntologyCassandraStoreImpl("test");
        } catch (UnknownHostException uhe) {
            uhe.printStackTrace();
        }
    }

    public CassandraQuery(String query) throws MalformedQueryException, ParseException {
        this(query, null);
    }

    public CassandraQuery(String query, String base) throws MalformedQueryException, ParseException {
        SPARQLParserFactory factory = new SPARQLParserFactory();
        ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(query);
        Map<String, String> prefixes = PrefixDeclProcessor.process(qc);
        QueryParser parser = factory.getParser();
        ParsedQuery parsedQuery = parser.parseQuery(query, base);
        expr = parsedQuery.getTupleExpr();
        System.out.println(expr);
        BindingSet bindings = new EmptyBindingSet();
        new BindingAssigner().optimize(expr, null, bindings);
        new CompareOptimizer().optimize(expr, null, bindings);
        new ConjunctiveConstraintSplitter().optimize(expr, null, bindings);
        new DisjunctiveConstraintOptimizer().optimize(expr, null, bindings);
        new SameTermFilterOptimizer().optimize(expr, null, bindings);
        new QueryModelNormalizer().optimize(expr, null, bindings);
        new QueryJoinOptimizer(new CassandraEvaluationStatistics()).optimize(expr, null,
                bindings);
        new IterativeEvaluationOptimizer().optimize(expr, null, bindings);
        new FilterOptimizer().optimize(expr, null, bindings);
        new OrderLimitOptimizer().optimize(expr, null, bindings);

        this.msparql = new CassandraSPARQLVisitor();

        expr.visit(msparql);

        mQuery = msparql.getMongoSPARQLQuery();
        mQuery.setPrefixes(prefixes);

        //parsedQuery.getTupleExpr().
        // int a = 1 + 1;
    }

    public void execute() {
        CassandraExecuter me = new CassandraExecuter(mQuery, mongoBDStore);
        me.execute();
        //MongoExecuter.execute();
    }

}

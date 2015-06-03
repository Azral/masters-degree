/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.cassandra.SPARQLQuery;

import extended.mongo.*;
import java.io.IOException;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;
import org.openrdf.sail.nativerdf.NativeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Арнольд
 */
public class CassandraEvaluationStatistics extends EvaluationStatistics {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    //private final NativeStore nativeStore;
    public CassandraEvaluationStatistics() {

    }

    /*public NativeEvaluationStatistics(NativeStore nativeStore) {
     this.nativeStore = nativeStore;
     }*/
    @Override
    protected EvaluationStatistics.CardinalityCalculator createCardinalityCalculator() {
        return new NativeCardinalityCalculator();
    }

    protected class NativeCardinalityCalculator extends EvaluationStatistics.CardinalityCalculator {

        @Override
        protected double getCardinality(StatementPattern sp) {
            //try {
                Value subj = getConstantValue(sp.getSubjectVar());
                if (!(subj instanceof Resource)) {
                    // can happen when a previous optimizer has inlined a comparison operator. 
                    // this can cause, for example, the subject variable to be equated to a literal value. 
                    // See SES-970 
                    subj = null;
                }
                Value pred = getConstantValue(sp.getPredicateVar());
                if (!(pred instanceof URI)) {
                    //  can happen when a previous optimizer has inlined a comparison operator. See SES-970 
                    pred = null;
                }
                Value obj = getConstantValue(sp.getObjectVar());
                Value context = getConstantValue(sp.getContextVar());
                if (!(context instanceof Resource)) {
                    //  can happen when a previous optimizer has inlined a comparison operator. See SES-970 
                    context = null;
                }
                return super.getCardinality(sp);
           // } catch (IOException e) {
            //    log.error(
            //            "Failed to estimate statement pattern cardinality, falling back to generic implementation",
            //            e);
            //    return super.getCardinality(sp);
            //}
        }

        protected Value getConstantValue(Var var) {
            return (var != null) ? var.getValue() : null;
        }
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.model;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import org.semanticweb.owlapi.model.IRI;

/**
 *
 * @author Арнольд
 */
public interface OWLOntologyMongoBDStore {

    public DBCollection getCollection(String collectionName);

    public void insertIntoCollection(DBCollection bCollection, BasicDBObject bObject);

    public void insertIndividual(BasicDBObject bObject);

    public void write(String text);

    public void clearDB();

    public void write(char c);

    public void flush();

    public void close();

    public DBCursor findIndividual(BasicDBObject bObject);

    public long countIndividuals(BasicDBObject bObject);

    public DBCursor findIndividualFields(BasicDBObject bObject, BasicDBObject fields);

}

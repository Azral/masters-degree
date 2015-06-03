/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.impl;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import extended.model.OWLOntologyMongoBDStore;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map.Entry;

/**
 *
 * @author Арнольд
 */
public class OWLOntologyMongoBDStoreImpl implements OWLOntologyMongoBDStore {

    MongoClient mongoClient;
    DB database;
    public static int mergeFlag = 0;
    static String base;

    public OWLOntologyMongoBDStoreImpl(List<ServerAddress> serverAddresses, String DatabaseName) throws UnknownHostException {
        this.mongoClient = new MongoClient(serverAddresses);
        this.database = mongoClient.getDB(DatabaseName);
    }

    public OWLOntologyMongoBDStoreImpl(String DatabaseName) throws UnknownHostException {
        this.mongoClient = new MongoClient();
        this.database = mongoClient.getDB(DatabaseName);
    }

    public DBCollection getCollection(String collectionName) {
        return database.getCollection(collectionName);
    }

    public static void setBase(String iri) {
        base = iri;
    }

    public void insertIntoCollection(DBCollection bCollection, BasicDBObject bObject) {
        bObject.put("_id", base);
        if (mergeFlag == 1) {
            BasicDBObject bdbo = (BasicDBObject) bCollection.findOne(new BasicDBObject("_id", base));
            if (bdbo != null) {
                merge((BasicDBObject) bObject.get("rdf:RDF"), (BasicDBObject) bdbo.get("rdf:RDF"));
            }
            //mergeFlag = 0;
        }
        bCollection.remove(new BasicDBObject("_id", base));
        bCollection.insert(bObject);
        //System.out.println(bObject);

    }

    public void clearDB() {
        DBCollection bCollection = getCollection("NamedIndividuals");
        bCollection.drop();
        bCollection = getCollection("test");
        bCollection.drop();
        // bCollection.drop();
    }

    public DBCursor findIndividual(BasicDBObject bObject) {
        DBCollection bCollection = getCollection("NamedIndividuals");
        DBCursor cursor = bCollection.find(bObject);
        return cursor;
    }

    public long countIndividuals(BasicDBObject bObject) {
        DBCollection bCollection = getCollection("NamedIndividuals");
        long count = bCollection.count(bObject);
        return count;
    }

    public DBCursor findIndividualFields(BasicDBObject bObject, BasicDBObject fields) {
        DBCollection bCollection = getCollection("NamedIndividuals");
        DBCursor cursor = bCollection.find(bObject, fields);
        return cursor;
    }

    public void insertIndividual(BasicDBObject bObject) {
        DBCollection bCollection = getCollection("NamedIndividuals");
        //System.out.println(bObject);
        try {
            bCollection.insert(bObject);
        } catch (DuplicateKeyException dke) {
            // bCollection.remove(new BasicDBObject("_id", bObject.get("_id")));
            // bCollection.insert(bObject);
        }
        // bCollection.drop();
    }

    public void write(String text) {

    }

    public void write(char c) {

    }

    public static void merge(BasicDBObject bdbo1, BasicDBObject bdbo2) {
        if (bdbo1 == null || bdbo2 == null) {
            return;
        }
        for (Entry<String, Object> entrySet : bdbo2.entrySet()) {
            Object currObject = bdbo1.get(entrySet.getKey());
            if (currObject != null) {
                if (currObject instanceof String) {
                    bdbo1.put(entrySet.getKey(), currObject);
                } else if (currObject instanceof BasicDBList && entrySet.getValue() instanceof BasicDBObject) {
                    BasicDBList bList = (BasicDBList) currObject;
                    bList.add(entrySet.getValue());
                } else if (currObject instanceof BasicDBObject && entrySet.getValue() instanceof BasicDBList) {
                    BasicDBList bList = (BasicDBList) entrySet.getValue();
                    BasicDBObject object = (BasicDBObject) currObject;
                    if (bList.contains(object)) {
                        bdbo1.put(entrySet.getKey(), bList);
                    } else {
                        bList.add(object);
                    }
                    bdbo1.put(entrySet.getKey(), bList);
                } else if (currObject instanceof BasicDBObject && entrySet.getValue() instanceof BasicDBObject) {
                    BasicDBList bList = new BasicDBList();
                    bList.add(currObject);
                    bList.add(entrySet.getValue());
                    bdbo1.put(entrySet.getKey(), bList);
                } else {
                    bdbo1.put(entrySet.getKey(), currObject);
                }
            } else {
                bdbo1.put(entrySet.getKey(), entrySet.getValue());
            }
        }
    }

    public void flush() {

        if (mongoClient != null) {
            mongoClient.close();

        }
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();

        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (mongoClient != null) {
            mongoClient.close();
            //database.
        }
        super.finalize(); //To change body of generated methods, choose Tools | Templates.
    }

    public void testData() {
        BasicDBObject bObject = new BasicDBObject("test", "test2");
        BasicDBObject bdbo = new BasicDBObject("tt", "tt2");
        bObject.append("ttt", bdbo);
        System.out.println(bObject);
        bdbo.append("tt3", "df");
        System.out.println(bObject);
    }

}

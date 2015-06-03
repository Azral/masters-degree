/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.example;

import extended.mongo.MongoQuery;

/**
 *
 * @author Арнольд
 */
public class testMongoSPARQL {

    static String queryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
            + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
            + "SELECT ?X ?Y\n"
            + "WHERE \n"
            + "{?X rdf:type ub:UndergraduateStudent .\n"
            + "  ?Y rdf:type ub:Course .\n"
            + "  ?X ub:takesCourse ?Y .\n"
            + "  <http://www.Department0.University0.edu/AssociateProfessor0>   \n"
            + "  	ub:teacherOf ?Y}";
    /*"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
     + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
     + "SELECT ?X ?Y ?Z\n"
     + "WHERE\n"
     + "{?X rdf:type ub:UndergraduateStudent .\n"
     + "  ?Z rdf:type ub:Course .\n"
     + "  ?X ub:advisor ?Y .\n"
     + "  ?Y ub:teacherOf ?Z .\n"
     + "  ?X ub:takesCourse ?Z}";*/
    /*"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
     + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
     + "SELECT ?X ?Y\n"
     + "WHERE \n"
     + "{?X rdf:type ub:UndergraduateStudent .\n"
     + "  ?Y rdf:type ub:Course .\n"
     + "  ?X ub:takesCourse ?Y .\n"
     + "  <http://www.Department0.University0.edu/AssociateProfessor0>   \n"
     + "  	ub:teacherOf ?Y}";*/

    /*"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
     + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
     + "SELECT ?X ?Y ?Z\n"
     + "WHERE\n"
     + "{?X rdf:type ub:GraduateStudent.\n"
     + "  ?Y rdf:type ub:University.\n"
     + "  ?Z rdf:type ub:Department.\n"
     + "  ?X ub:memberOf ?Z.\n"
     + "  ?Z ub:subOrganizationOf ?Y.\n"
     + "  ?X ub:undergraduateDegreeFrom ?Y}"; */
    /*"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
     + "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n"
     + "SELECT ?X ?Y ?Z	\n"
     + "WHERE\n"
     + "{?X rdf:type ?Y .\n"
     + "  ?X ub:takesCourse ?Z."
     + "?X ub:takesCourse\n"
     + "<http://www.Department0.University0.edu/GraduateCourse64>}";*/
    public static void main(String[] args) {
        try {
            MongoQuery mq = new MongoQuery(queryString);
            long start = System.currentTimeMillis();
            mq.execute();
            long stop = System.currentTimeMillis();
            System.out.println(stop - start);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

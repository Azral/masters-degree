/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package extended.impl;

/*
 * This file is part of the OWL API.
 *
 * The contents of this file are subject to the LGPL License, Version 3.0.
 *
 * Copyright (C) 2011, The University of Manchester
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/.
 *
 *
 * Alternatively, the contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2011, University of Manchester
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import extended.model.OWLOntologyManagerNoSQL;
import extended.rdf.rdfxml.RDFXMLOntologyCassandraStorer;
import extended.rdf.rdfxml.RDFXMLOntologyMongoDBStorer;
import java.io.IOException;
import java.io.Serializable;
import org.semanticweb.owlapi.io.OWLOntologyStorageIOException;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyFactory;
import org.semanticweb.owlapi.model.OWLOntologyFormat;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.semanticweb.owlapi.model.OWLOntologyStorer;
import org.semanticweb.owlapi.model.OWLOntologyStorerNotFoundException;
import org.semanticweb.owlapi.model.UnknownOWLOntologyException;
import uk.ac.manchester.cs.owl.owlapi.OWLOntologyManagerImpl;

/**
 * @author Matthew Horridge, The University Of Manchester, Bio-Health
 * Informatics Group, Date: 27-Oct-2006
 */
public class OWLOntologyManagerNoSQLImpl extends OWLOntologyManagerImpl implements OWLOntologyManagerNoSQL,
        OWLOntologyFactory.OWLOntologyCreationHandler, Serializable {

    protected final OWLOntologyStorer ontologyMongoDBStore = new RDFXMLOntologyMongoDBStorer();
    protected final OWLOntologyStorer ontologyCassandraStore = new RDFXMLOntologyCassandraStorer();

    /**
     * @param dataFactory data factory
     */
    public OWLOntologyManagerNoSQLImpl(OWLDataFactory dataFactory) {
        super(dataFactory);
    }

    // /////////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // Methods to save ontologies
    //
    // /////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void saveOntologyMongoDB(OWLOntology ontology)
            throws OWLOntologyStorageException, UnknownOWLOntologyException {
        OWLOntologyFormat format = getOntologyFormat(ontology);
        saveOntologyMongoDB(ontology, format);
    }

    public void saveOntologyMongoDB(OWLOntology ontology,
            IRI iri)
            throws OWLOntologyStorageException, UnknownOWLOntologyException {
        OWLOntologyFormat format = getOntologyFormat(ontology);
        //IRI documentIRI = getOntologyDocumentIRI(ontology);
        try {
            if (ontologyMongoDBStore.canStoreOntology(format)) {
                ontologyMongoDBStore.storeOntology(ontology, iri, format);
                return;
            }
            throw new OWLOntologyStorerNotFoundException(format);
        } catch (IOException e) {
            throw new OWLOntologyStorageIOException(e);
        }
    }

    public void saveOntologyMongoDB(OWLOntology ontology,
            OWLOntologyFormat ontologyFormat)
            throws OWLOntologyStorageException, UnknownOWLOntologyException {
        OWLOntologyFormat format = getOntologyFormat(ontology);
        IRI documentIRI = getOntologyDocumentIRI(ontology);
        try {
            if (ontologyMongoDBStore.canStoreOntology(ontologyFormat)) {
                ontologyMongoDBStore.storeOntology(ontology, documentIRI, ontologyFormat);
                return;
            }
            throw new OWLOntologyStorerNotFoundException(ontologyFormat);
        } catch (IOException e) {
            throw new OWLOntologyStorageIOException(e);
        }
    }

    public void saveOntologyCassandra(OWLOntology ontology)
            throws OWLOntologyStorageException, UnknownOWLOntologyException {
        OWLOntologyFormat format = getOntologyFormat(ontology);
        saveOntologyCassandra(ontology, format);
    }

    public void saveOntologyCassandra(OWLOntology ontology,
            IRI iri)
            throws OWLOntologyStorageException, UnknownOWLOntologyException {
        OWLOntologyFormat format = getOntologyFormat(ontology);
        //IRI documentIRI = getOntologyDocumentIRI(ontology);
        try {
            if (ontologyCassandraStore.canStoreOntology(format)) {
                ontologyCassandraStore.storeOntology(ontology, iri, format);
                return;
            }
            throw new OWLOntologyStorerNotFoundException(format);
        } catch (IOException e) {
            throw new OWLOntologyStorageIOException(e);
        }
    }

    public void saveOntologyCassandra(OWLOntology ontology,
            OWLOntologyFormat ontologyFormat)
            throws OWLOntologyStorageException, UnknownOWLOntologyException {
        OWLOntologyFormat format = getOntologyFormat(ontology);
        IRI documentIRI = getOntologyDocumentIRI(ontology);
        try {
            if (ontologyCassandraStore.canStoreOntology(ontologyFormat)) {
                ontologyCassandraStore.storeOntology(ontology, documentIRI, ontologyFormat);
                return;
            }
            throw new OWLOntologyStorerNotFoundException(ontologyFormat);
        } catch (IOException e) {
            throw new OWLOntologyStorageIOException(e);
        }
    }

}

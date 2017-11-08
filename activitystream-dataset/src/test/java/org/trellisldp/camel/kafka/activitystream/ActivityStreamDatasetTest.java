/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trellisldp.camel.kafka.activitystream;

import static org.apache.camel.util.ObjectHelper.loadResourceAsURL;

import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;

/**
 * org.trellisldp.camel.kafka.activitystream.ActivityStreamDatasetTest.
 *
 * @author christopher-johnson
 */
public class ActivityStreamDatasetTest {
    private static final JenaRDF rdf = new JenaRDF();

    @BeforeAll
    public void setUp() {

    }

    @Test
    public void getDataSetTest() {
        final Dataset dataset =
                TDBFactory.assembleDataset(loadResourceAsURL("/jena.dataset.ttl").toString());
        dataset.begin(ReadWrite.READ);
        final Model model = dataset.getDefaultModel();
        final String q = "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o}";
        final Query query = QueryFactory.create(q);
        final QueryExecution qexec = QueryExecutionFactory.create(query, model);
        final Model results = qexec.execConstruct();
        final Graph newGraph = rdf.asGraph(results);
    }
}

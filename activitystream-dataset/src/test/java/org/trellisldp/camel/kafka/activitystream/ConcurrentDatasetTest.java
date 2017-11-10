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

import static junit.framework.TestCase.assertNotNull;

import java.util.function.Consumer;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.junit.jupiter.api.Test;

class ConcurrentDatasetTest {

    @Test
    void query_select_01() {
        query("http://localhost:3330/rdf/query", "SELECT * { ?s ?p ?o}", qExec -> {
            ResultSet rs = qExec.execSelect();
            String set = ResultSetFormatter.asText(rs);
            assertNotNull(set);
        });
    }

    private static void query(String url, String query, Consumer<QueryExecution> body) {
        try (QueryExecution qExec = QueryExecutionFactory.sparqlService(url, query)) {
            body.accept(qExec);
        }
    }
}

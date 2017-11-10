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

import static java.util.Objects.requireNonNull;
import static org.apache.jena.graph.Factory.createDefaultGraph;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFSyntax;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.atlas.AtlasException;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RiotException;
import org.trellisldp.api.IOService;
import org.trellisldp.api.RuntimeRepositoryException;

public class JenaIOService implements IOService {
    private static final JenaRDF rdf = new JenaRDF();

    @Override
    public void write(Stream<? extends Triple> triples, OutputStream output, RDFSyntax syntax,
                      IRI... profiles) {
    }

    public Stream<? extends Triple> read(final InputStream input, final String base,
                                         final RDFSyntax syntax) {
        requireNonNull(input, "The input stream may not be null!");
        requireNonNull(syntax, "The syntax value may not be null!");

        try {
            final org.apache.jena.graph.Graph graph = createDefaultGraph();
            final Lang lang = rdf.asJenaLang(syntax).orElseThrow(
                    () -> new RuntimeRepositoryException(
                            "Unsupported RDF Syntax: " + syntax.mediaType));

            RDFParser.source(input).lang(lang).base(base).parse(graph);

            return rdf.asGraph(graph).stream();
        } catch (final RiotException | AtlasException ex) {
            throw new RuntimeRepositoryException(ex);
        }
    }

    @Override
    public void update(Graph graph, String update, String context) {

    }
}

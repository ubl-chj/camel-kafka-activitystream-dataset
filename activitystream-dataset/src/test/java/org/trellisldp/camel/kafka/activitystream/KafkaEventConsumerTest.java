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

import static org.apache.camel.LoggingLevel.INFO;
import static org.apache.commons.rdf.api.RDFSyntax.JSONLD;

import java.io.InputStream;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.commons.rdf.jena.JenaGraph;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.fuseki.embedded.FusekiServer;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.TDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.IOService;
import org.trellisldp.camel.ActivityStreamProcessor;

/**
 * org.trellisldp.camel.kafka.activitystream.KafkaEventConsumerTest.
 *
 * @author christopher-johnson
 */
public final class KafkaEventConsumerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEventConsumerTest.class);
    private static final JenaRDF rdf = new JenaRDF();
    private static IOService service = new JenaIOService();
    private static DatasetGraph dsg = dataset();

    private KafkaEventConsumerTest() {
    }

    private static DatasetGraph dataset() {
        return TDBFactory.createDatasetGraph("/tmp/activityStream_data");
    }

    private static void startFuseki(DatasetGraph ds) {
        LOGGER.info("Starting Fuseki");
        FusekiServer server = FusekiServer.create().add("/rdf", ds, true).build();
        server.start();
    }

    public static void main(final String[] args) throws Exception {
        startFuseki(dsg);

        LOGGER.info("About to run Kafka-camel integration...");

        final CamelContext camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new RouteBuilder() {
            public void configure() {
                final PropertiesComponent pc =
                        getContext().getComponent("properties", PropertiesComponent.class);
                pc.setLocation("classpath:application.properties");

                LOGGER.info("About to start route: Kafka Server -> Log ");

                from("kafka:{{consumer.topic}}?brokers={{kafka.host}}:{{kafka.port}}"
                        + "&maxPollRecords={{consumer.maxPollRecords}}"
                        + "&consumersCount={{consumer.consumersCount}}"
                        + "&seekTo={{consumer.seekTo}}" + "&groupId={{consumer.group}}")
                        .routeId("FromKafka").unmarshal().json(JsonLibrary.Jackson)
                        .setHeader("fuseki.base", constant("{{fuseki.base}}"))
                        .process(new ActivityStreamProcessor()).marshal()
                        .json(JsonLibrary.Jackson, true)
                        .log(INFO, LOGGER, "Serializing ActivityStreamMessage to JSONLD")
                        //.to("file://{{serialization.log}}");
                        .to("direct:jena");
                from("direct:jena").process(exchange -> {
                    final JenaGraph graph = rdf.createGraph();
                    service.read(exchange.getIn().getBody(InputStream.class), null, JSONLD)
                            .forEach(graph::add);
                    try (RDFConnection conn = RDFConnectionFactory
                            .connect(exchange.getIn().getHeader("fuseki.base").toString())) {
                        Txn.executeWrite(conn, () -> {
                            conn.load(graph.asJenaModel());
                        });
                        conn.commit();
                    }
                    LOGGER.info("Committing ActivityStreamMessage to Jena Dataset");
                });
            }
        });
        camelContext.start();

        // let it run for 5 minutes before shutting down
        Thread.sleep(5 * 60 * 1000);

        camelContext.stop();
    }
}
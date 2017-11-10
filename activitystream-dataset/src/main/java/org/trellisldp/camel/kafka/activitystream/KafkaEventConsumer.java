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
import static org.slf4j.LoggerFactory.getLogger;

import java.io.InputStream;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.main.Main;
import org.apache.camel.main.MainListenerSupport;
import org.apache.camel.main.MainSupport;
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
import org.trellisldp.api.IOService;
import org.trellisldp.camel.ActivityStreamProcessor;

/**
 * KafkaEventConsumer.
 *
 * @author christopher-johnson
 */
public class KafkaEventConsumer {
    private static final Logger LOGGER = getLogger(KafkaEventConsumer.class);
    private static DatasetGraph dsg = dataset();

    private static DatasetGraph dataset() {
        return TDBFactory.createDatasetGraph("/tmp/activityStream_data");
    }

    private static void startFuseki(DatasetGraph dsg) {
        LOGGER.info("Starting Fuseki");
        FusekiServer server = FusekiServer.create().add("/rdf", dsg, true).build();
        server.start();
    }

    public static void main(String[] args) throws Exception {
        startFuseki(dsg);
        KafkaEventConsumer kafkaConsumer = new KafkaEventConsumer();
        kafkaConsumer.init();
    }

    private void init() throws Exception {
        Main main = new Main();
        main.addRouteBuilder(new KafkaEventRoute());
        main.addMainListener(new Events());
        main.run();
    }

    public static class Events extends MainListenerSupport {

        @Override
        public void afterStart(MainSupport main) {
            System.out.println("KafkaEventConsumer with Camel is now started!");
        }

        @Override
        public void beforeStop(MainSupport main) {
            System.out.println("KafkaEventConsumer with Camel is now being stopped!");
        }
    }

    public static class KafkaEventRoute extends RouteBuilder {

        private static final JenaRDF rdf = new JenaRDF();
        private static IOService service = new JenaIOService();

        /**
         * Configure the event route.
         *
         * @throws Exception exception
         */
        public void configure() throws Exception {
            final PropertiesComponent pc =
                    getContext().getComponent("properties", PropertiesComponent.class);
            pc.setLocation("classpath:cfg/org.trellisldp.camel.kafka.activitystream.cfg");

            from("kafka:{{consumer.topic}}?brokers={{kafka.host}}:{{kafka.port}}"
                    + "&maxPollRecords={{consumer.maxPollRecords}}"
                    + "&consumersCount={{consumer.consumersCount}}" + "&seekTo={{consumer.seekTo}}"
                    + "&groupId={{consumer.group}}").routeId("kafkaConsumer").unmarshal()
                    .json(JsonLibrary.Jackson).setHeader("fuseki.base", constant("{{fuseki.base}}"))
                    .process(new ActivityStreamProcessor()).marshal()
                    .json(JsonLibrary.Jackson, true)
                    .log(INFO, LOGGER, "Serializing ActivityStreamMessage to JSONLD")
                    .to("direct:jena");
            from("direct:jena").routeId("jenaDataset").routeId("jenaDataset").process(exchange -> {
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
    }
}
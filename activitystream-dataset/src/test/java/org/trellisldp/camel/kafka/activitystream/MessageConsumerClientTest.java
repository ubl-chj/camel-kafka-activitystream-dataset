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
import static org.apache.commons.rdf.api.RDFSyntax.JSONLD;

import java.io.InputStream;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.commons.rdf.jena.JenaGraph;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.tdb.TDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trellisldp.api.IOService;
import org.trellisldp.camel.ActivityStreamProcessor;
import org.trellisldp.io.JenaIOService;

/**
 * org.trellisldp.camel.kafka.activitystream.MessageConsumerClientTest.
 *
 * @author christopher-johnson
 */
public final class MessageConsumerClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(MessageConsumerClientTest.class);
    private static final JenaRDF rdf = new JenaRDF();
    private static IOService service = new JenaIOService(null);

    private MessageConsumerClientTest() {
    }

    public static void main(final String[] args) throws Exception {

        LOG.info("About to run Kafka-camel integration...");

        final CamelContext camelContext = new DefaultCamelContext();

        camelContext.addRoutes(new RouteBuilder() {
            public void configure() {
                final PropertiesComponent pc =
                        getContext().getComponent("properties", PropertiesComponent.class);
                pc.setLocation("classpath:application.properties");
                final Dataset dataset = TDBFactory
                        .assembleDataset(loadResourceAsURL("/jena.dataset.ttl").toString());

                log.info("About to start route: Kafka Server -> Log ");

                from("kafka:{{consumer.topic}}?brokers={{kafka.host}}:{{kafka.port}}"
                        + "&maxPollRecords={{consumer.maxPollRecords}}"
                        + "&consumersCount={{consumer.consumersCount}}"
                        + "&seekTo={{consumer.seekTo}}" + "&groupId={{consumer.group}}")
                        .routeId("FromKafka").unmarshal().json(JsonLibrary.Jackson)
                        .process(new ActivityStreamProcessor()).marshal()
                        .json(JsonLibrary.Jackson, true)
                        //.to("file://{{serialization.log}}");
                        .to("direct:jena");
                from("direct:jena").process(exchange -> {
                    dataset.begin(ReadWrite.WRITE);
                    try {
                        final JenaGraph graph = rdf.createGraph();
                        service.read(exchange.getIn().getBody(InputStream.class), null, JSONLD)
                                .forEach(graph::add);
                        final Model model = dataset.getDefaultModel();
                        model.add(graph.asJenaModel());
                        dataset.commit();
                    } finally {
                        dataset.end();
                    }
                });
            }
        });
        camelContext.start();

        // let it run for 5 minutes before shutting down
        Thread.sleep(5 * 60 * 1000);

        camelContext.stop();
    }

}
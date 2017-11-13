## camel-kafka-activitystream-dataset

An implementation of [camel-ldp](https://github.com/trellis-ldp/camel-ldp) that consumes an activity stream from Kafka 
and writes the events to a Jena dataset.

### Build
```bash
$ gradle build
$ gradle docker
```
### Configuration

* org.trellisldp.camel.kafka.activitystream.cfg

* Default dataset Location `/tmp/activityStream_data`

 ### Docker
 ```bash
 $ docker run -ti trellisldp/activitystream-dataset
 ```
 
### Use With Trellis:
* Start [trellis-compose](https://github.com/trellis-ldp/trellis-deployment/blob/master/trellis-compose/docker-compose.yml) 

* Start [camel-integration-compose](https://github.com/trellis-ldp/trellis-deployment/blob/master/trellis-compose/camel-integrations/docker-compose.yml)

### Fuseki Endpoint
* This includes an embedded Fuseki instance
* Read and query events at:
`http://localhost:3330/rdf/data?graph=http://trellisldp.org/activitystream`

#### Example Query
* Get Resource Subjects by Type (e.g. `<http://iiif.io/api/presentation/2#Manifest>`)

```bash
$ curl -v http://localhost:3330/rdf/query?query=SELECT%20*%20WHERE%20%7B%3Fs%20%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type%3E%20%3Chttp%3A%2F%2Fiiif.io%2Fapi%2Fpresentation%2F2%23Manifest%3E%7D
```
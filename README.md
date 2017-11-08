## camel-kafka-activitystream-dataset

An implementation of [camel-ldp](https://github.com/trellis-ldp/camel-ldp) that consumes an activity stream from Kafka 
and writes the events to a Jena dataset.

### Build
```bash
$ gradle install
$ gradle copyTask
$ gradle docker
```
### Configuration

* org.trellisldp.camel.kafka.activitystream.cfg
* jena.dataset.ttl

Change location of the dataset in `jena.dataset.ttl`:
```turtle
 tdb:location "/tmp/activityStream_data" .
 ```
 
 ### Docker
 ```bash
 $ docker run -ti trellisldp/camel-kafka-activitystream-dataset
 ```
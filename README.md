Analysis of Open Street Map using GeoMesa

This sample project demonstrates how to analyze OSM using GeoMesa.

To ingest: 

1. Build the project
   ```
   mvn clean install
   ```

2. Submit topology
   ```
   storm jar geomesa-osm-1.0-SNAPSHOT.jar geomesa.osm.OSMIngest -instanceId [instanceId] -zookeepers [zookeepers] -user [user] -password [password] -auths [auths] -tableName [tableName] -featureName [featureName] -topic [kafka topic name]
   ```

3. Create kafka topic
   ```
   kafka-create-topic.sh --zookeeper [zookeepers] --replica 3 --partition 10 --topic [kafka topic name]
   ```

4. Produce kafka messages from ingest file
   ```
   java -cp geomesa-osm-1.0.0-SNAPSHOT.jar geomesa.osm.OSMIngestProducer -ingestFile [ingestFile] -topic [kafka topic name] -brokers [kafka broker list]
   ```

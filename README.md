Analysis of Open Street Map using GeoMesa

This sample project demonstrates how to analyze OSM using GeoMesa.

To ingest: 

1. Build the project
   ```
   mvn install
   ```

2. Submit topology
   ```
   storm jar geomesa-osm-1.0.0.jar geomesa.osm.OSMIngest -instanceId [instanceId] -zookeepers [zookeepers] -user [user] -password [password] -auths [auths] -tableName [tableName] -featureName [featureName]
   ```

3. Create kafka topic
   ```
   kafka-create-topic.sh --zookeeper [zookeepers] --replica 1 --partition 1 --topic OSMIngest
   ```

4. Produce kafka messages from ingest file
   ```
   java -cp geomesa-osm-1.0.0.jar geomesa.osm.OSMIngestProducer -ingestFile [ingestFile] -zookeepers [zookeepers]
   ```

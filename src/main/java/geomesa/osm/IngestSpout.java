package geomesa.osm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class IngestSpout extends BaseRichSpout {
  ConsumerIterator<String, String> kafkaIterator = null;
  Map<String, String> conf;
  String groupId;
  String topic;
  String featureName;
  Map<String , String> connectionParams;
  private FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter = null;
  private SimpleFeatureBuilder featureBuilder;
  private GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();


  private static final int LATITUDE_COL_IDX  = 0;
  private static final int LONGITUDE_COL_IDX = 1;

  public IngestSpout(Map<String, String> conf, String groupId, String topic) throws IOException {
    this.conf = conf;
    this.groupId = groupId;
    this.topic = topic;

    featureName = conf.get(OSMIngest.FEATURE_NAME);

    connectionParams = new HashMap<String , String>();
    connectionParams.put("instanceId", conf.get(OSMIngest.INSTANCE_ID));
    connectionParams.put("zookeepers", conf.get(OSMIngest.ZOOKEEPERS));
    connectionParams.put("user", conf.get(OSMIngest.USER));
    connectionParams.put("password", conf.get(OSMIngest.PASSWORD));
    connectionParams.put("auths", conf.get(OSMIngest.AUTHS));
    connectionParams.put("tableName", conf.get(OSMIngest.TABLE_NAME));
  }

  public void nextTuple() {
    if(kafkaIterator.hasNext()) {

      final String[] attributes = kafkaIterator.next().message().split(",");

      // Only ingest attributes that have a latitude and longitude
      if (attributes[LATITUDE_COL_IDX] == null || attributes[LONGITUDE_COL_IDX] == null) {
        //log.info("Line did not have valid Latitude and Longitude");
        return;
      }

      featureBuilder.reset();
      featureBuilder.addAll(attributes);
      final SimpleFeature simpleFeature = featureBuilder.buildFeature("fid");

      simpleFeature.setDefaultGeometry(getGeometry(attributes));

      try {
        final SimpleFeature next = featureWriter.next();
        for (int i = 0; i < simpleFeature.getAttributeCount(); i++) {
          next.setAttribute(i, simpleFeature.getAttribute(i));
        }
        featureWriter.write();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private Geometry getGeometry(final String[] attributes) {
    try {
      final Double lat = (double)Integer.parseInt(attributes[LATITUDE_COL_IDX]) / 10e7;
      final Double lon = (double)Integer.parseInt(attributes[LONGITUDE_COL_IDX]) / 10e7;
      return geometryFactory.createPoint(new Coordinate(lon, lat));
    } catch (NumberFormatException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    Properties props = new Properties();
    props.put("zookeeper.connect", conf.get("zookeepers"));
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder(new VerifiableProperties()), new StringDecoder(new VerifiableProperties()));
    List<KafkaStream<String, String>> streams = consumerMap.get(topic);
    KafkaStream<String, String> stream = null;
    if (streams.size() == 1) {
      stream = streams.get(0);
    } else {
      try {
        throw new Exception("Streams should be of size 1");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    kafkaIterator = stream.iterator();

    final DataStore ds;
    try {
      ds = DataStoreFinder.getDataStore(connectionParams);
      SimpleFeatureType featureType = ds.getSchema(featureName);
      featureBuilder = new SimpleFeatureBuilder(featureType);
      featureWriter = ds.getFeatureWriter(featureName, Transaction.AUTO_COMMIT);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

package geomesa.osm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OSMIngest {
  static final String TOPIC = "OSMIngest";
  static final String INSTANCE_ID = "instanceId";
  static final String ZOOKEEPERS = "zookeepers";
  static final String USER = "user";
  static final String PASSWORD = "password";
  static final String AUTHS = "auths";
  static final String TABLE_NAME = "tableName";
  static final String FEATURE_NAME = "featureName";

  static final String[] ACCUMULO_CONNECTION_PARAMS =
    new String[] {
      INSTANCE_ID, ZOOKEEPERS, USER, PASSWORD, AUTHS, TABLE_NAME
    };

  static Options getCommonRequiredOptions() {
    Options options = new Options();
    Option instanceIdOpt =
      OptionBuilder.withArgName(INSTANCE_ID)
        .hasArg()
        .isRequired()
        .withDescription("accumulo connection parameter instanceId")
        .create(INSTANCE_ID);
    Option zookeepersOpt =
      OptionBuilder.withArgName(ZOOKEEPERS)
        .hasArg()
        .isRequired()
        .withDescription("accumulo connection parameter zookeepers")
        .create(ZOOKEEPERS);
    Option userOpt =
      OptionBuilder.withArgName(USER)
        .hasArg()
        .isRequired()
        .withDescription("accumulo connection parameter user")
        .create(USER);
    Option passwordOpt =
      OptionBuilder.withArgName(PASSWORD)
        .hasArg()
        .isRequired()
        .withDescription("accumulo connection parameter password")
        .create(PASSWORD);
    Option authsOpt =
      OptionBuilder.withArgName(AUTHS)
        .hasArg()
        .isRequired()
        .withDescription("accumulo connection parameter auths")
        .create(AUTHS);
    Option tableNameOpt =
      OptionBuilder.withArgName(TABLE_NAME)
        .hasArg()
        .isRequired()
        .withDescription("accumulo connection parameter tableName")
        .create(TABLE_NAME);
    Option featureNameOpt =
      OptionBuilder.withArgName(FEATURE_NAME)
        .hasArg()
        .isRequired()
        .withDescription("name of feature in accumulo table")
        .create(FEATURE_NAME);
    options.addOption(instanceIdOpt);
    options.addOption(zookeepersOpt);
    options.addOption(userOpt);
    options.addOption(passwordOpt);
    options.addOption(authsOpt);
    options.addOption(tableNameOpt);
    options.addOption(featureNameOpt);
    return options;
  }

  public static Map<String, String> getAccumuloDataStoreConf(CommandLine cmd) {
    Map<String , String> dsConf = new HashMap<String , String>();
    for (String param : ACCUMULO_CONNECTION_PARAMS) {
      dsConf.put(param, cmd.getOptionValue(param));
    }
    return dsConf;
  }

  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static int run(String[] args) throws Exception {
    CommandLineParser parser = new BasicParser();
    Options options = getCommonRequiredOptions();

    CommandLine cmd = parser.parse( options, args);
    Map<String, String> dsConf = getAccumuloDataStoreConf(cmd);

    String featureName = cmd.getOptionValue(FEATURE_NAME);
    SimpleFeatureType featureType = buildOSMFeatureType(featureName);

    DataStore ds = DataStoreFinder.getDataStore(dsConf);
    ds.createSchema(featureType);
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    String topic = TOPIC;
    String groupId = TOPIC;
    dsConf.put(OSMIngest.FEATURE_NAME, featureName);
    IngestSpout ingestSpout = new IngestSpout(dsConf, groupId, topic);
    topologyBuilder.setSpout(TOPIC, ingestSpout);
    Config stormConf = new Config();
    stormConf.setNumWorkers(1);
    stormConf.setDebug(true);
    StormSubmitter.submitTopology(TOPIC, stormConf, topologyBuilder.createTopology());
    return 1;
  }


  private static SimpleFeatureType buildOSMFeatureType(String featureName) throws SchemaException {
    String spec = Joiner.on(",").join(attributes);
    return DataUtilities.createType(featureName, spec);
  }

  static final List<String> attributes = Lists.newArrayList(
    "lat:Integer",
    "lon:Integer",
    "*geom:Point:srid=4326"
  );

}

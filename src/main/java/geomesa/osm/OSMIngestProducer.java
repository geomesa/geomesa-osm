package geomesa.osm;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class OSMIngestProducer {
  static String INGEST_FILE = "ingestFile";
  static String ZOOKEEPERS = "zookeepers";

  public static Options getRequiredOptions() {
    Options options = new Options();
    Option ingestFileOpt = OptionBuilder.withArgName(INGEST_FILE)
      .hasArg()
      .isRequired()
      .withDescription("ingest csv file")
      .create(INGEST_FILE);
    Option zookeepersOpt = OptionBuilder.withArgName(ZOOKEEPERS)
      .hasArg()
      .isRequired()
      .withDescription("zookeepers")
      .create(ZOOKEEPERS);
    options.addOption(ingestFileOpt);
    options.addOption(zookeepersOpt);
    return options;
  }

  public static void main(String[] args) {

    try {

      CommandLineParser parser = new BasicParser();
      Options options = getRequiredOptions();

      CommandLine cmd = parser.parse( options, args);
      final kafka.javaapi.producer.Producer<Integer, String> producer;
      final String topic = OSMIngest.TOPIC;
      final Properties props = new Properties();
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      props.put("metadata.broker.list", "localhost:9092"); //default
      props.put("zookeeper.connect", cmd.getOptionValue(ZOOKEEPERS));
      producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));

      FileReader fileReader = new FileReader(cmd.getOptionValue(INGEST_FILE));
      BufferedReader bufferedReader = new BufferedReader(fileReader);

      for (String x = bufferedReader.readLine();
           x != null;
           x = bufferedReader.readLine()) {
        producer.send(new KeyedMessage<Integer, String>(topic, x));
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}

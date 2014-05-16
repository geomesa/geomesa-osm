package geomesa.osm;

import com.google.common.base.Joiner;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class OSMIngestProducer {
    static String INGEST_FILE = "ingestFile";
    static String TOPIC = "topic";
    static String BROKER_LIST = "brokers";

    public static Options getRequiredOptions() {
        Options options = new Options();
        Option ingestFileOpt = OptionBuilder.withArgName(INGEST_FILE)
            .hasArg()
            .isRequired()
            .withDescription("ingest csv file")
            .create(INGEST_FILE);
        Option topicOpt = OptionBuilder.withArgName(TOPIC)
            .hasArg()
            .isRequired()
            .withDescription("name of kafka topic")
            .create(TOPIC);
        Option brokersOpt = OptionBuilder.withArgName(BROKER_LIST)
            .hasArg()
            .isRequired()
            .withDescription("kafka metadata brokers list")
            .create(BROKER_LIST);
        options.addOption(ingestFileOpt);
        options.addOption(topicOpt);
        options.addOption(brokersOpt);
        return options;
    }

    public static void main(String[] args) {

        try {

            CommandLineParser parser = new BasicParser();
            Options options = getRequiredOptions();

            CommandLine cmd = parser.parse( options, args);
            final kafka.javaapi.producer.Producer<String, String> producer;
            final String topic = cmd.getOptionValue(TOPIC);
            final Properties props = new Properties();
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("metadata.broker.list", cmd.getOptionValue(BROKER_LIST));
            props.put("producer.type", "async");
            producer = new Producer(new ProducerConfig(props));

            FileReader fileReader = new FileReader(cmd.getOptionValue(INGEST_FILE));
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            List<String> agglomeratedData = new ArrayList<String>();
            //to assign messages to different partitions using default partitioner, need random key
            Random rnd = new Random();
            for (String x = bufferedReader.readLine();
                x != null;
                x = bufferedReader.readLine()) {

                agglomeratedData.add(x);
                if (agglomeratedData.size() == 40) {
                    producer.send(new KeyedMessage<String, String>(topic, String.valueOf(rnd.nextInt()), Joiner.on("%").join(agglomeratedData)));
                    agglomeratedData = new ArrayList<String>();
                }
            }
            if (agglomeratedData.size() > 0) {
                producer.send(new KeyedMessage<String, String>(topic, String.valueOf(rnd.nextInt()), Joiner.on("%").join(agglomeratedData)));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

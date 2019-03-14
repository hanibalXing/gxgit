package cn.gx.storm.trident2.kafka;

import org.apache.storm.Config;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

import static cn.gx.storm.utils.Runner.runThenStop;


public class ProducerKafkaTopology
{
    public static void main(String[] args) throws InterruptedException
    {
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", "1"),
                new Values("trident", "1"),
                new Values("needs", "1"),
                new Values("javadoc", "1")
        );
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withProducerProperties(props)
                .withKafkaTopicSelector(new DefaultTopicSelector("storm-inter"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
        topology.newStream("spout", spout).parallelismHint(1)
                .localOrShuffle()
                .partitionPersist(stateFactory, fields, new TridentKafkaStateUpdater(), new Fields())
                .parallelismHint(3);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(5);
        runThenStop("test", conf, topology.build(), 60);
    }
}

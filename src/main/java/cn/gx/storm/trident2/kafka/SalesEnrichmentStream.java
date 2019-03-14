package cn.gx.storm.trident2.kafka;

import org.apache.storm.Config;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static cn.gx.storm.utils.Runner.runThenStop;


public class SalesEnrichmentStream
{
    private final static Logger LOG = LoggerFactory.getLogger(SalesEnrichmentStream.class);

    public static void main(String[] args) throws InterruptedException
    {
        KafkaSpoutConfig<String, String> consumerBuilder = build();

        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withProducerProperties(createProducerProps())
                .withKafkaTopicSelector(new DefaultTopicSelector("salary"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "salary"));

        LOG.info("The consumer configuration: {}", consumerBuilder.getKafkaProps());
        final TridentTopology trident = new TridentTopology();
        trident.newStream("salesStream", new KafkaTridentSpoutOpaque<>(consumerBuilder)).parallelismHint(3)
                .peek(input -> LOG.warn("From Kafka: {}-{}", input.getFields(), input))
                .localOrShuffle()
                .map(new SimpleMapFunction(), new Fields("key", "salary"))
                .partitionPersist(stateFactory, new Fields("key", "salary"), new TridentKafkaStateUpdater(), new Fields())
                .parallelismHint(3);


        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(5);
        runThenStop("SalesTopology", conf, trident.build(), 60);
    }

    private static class SimpleMapFunction implements MapFunction
    {
        @Override
        public Values execute(TridentTuple input)
        {
            final String key = input.getStringByField("key");
            final String value = input.getStringByField("value");
            try
            {
                String[] values = value.split(",");
                LOG.info("SimpleMapFunction {}-{}-{}", value, values[0], values[1]);
                int salary = Integer.parseInt(values[0]) * Integer.parseInt(values[1]);
                return new Values(key, String.valueOf(salary));
            } catch (Throwable e)
            {
                return new Values(key, "0");
            }
        }
    }

    private static KafkaSpoutConfig<String, String> build()
    {
        return KafkaSpoutConfig.builder("192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092", "test10")
                .setProp("auto.offset.reset", "earliest")
                .setProp("group.id", "g7")
                .setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .setOffsetCommitPeriodMs(3)
                .build();
    }

    private static Properties createProducerProps()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.88.108:9092,192.168.88.109:9092,192.168.88.110:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}

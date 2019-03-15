package cn.gx.storm.trident2.kafka;

import org.apache.storm.Config;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.trident.TridentTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cn.gx.storm.utils.Runner.runThenStop;


public class ConsumerKafkaTopology {
	private final static Logger LOG = LoggerFactory.getLogger(ConsumerKafkaTopology.class);

	public static void main(String[] args) throws InterruptedException {
		final TridentTopology trident = new TridentTopology();
		KafkaSpoutConfig<String, String> build = KafkaSpoutConfig.builder("node1:9092,node2:9092,node3:9092", "test")
				.setProp("auto.offset.reset", "earliest")
				.setProp("group.id", "g5")
				.setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.build();
		trident.newStream("testStream2", new KafkaTridentSpoutOpaque<>(build)).parallelismHint(3)
				.peek(input -> LOG.warn("{}-{}", input.getFields(), input));

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(5);
		runThenStop("testTopology3", conf, trident.build(), 60);
	}
}

package cn.gx.storm.trident2.kafka;

import org.apache.storm.Config;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static cn.gx.storm.utils.Runner.runThenStop;
public class ConsumerKafkaTopology {
	private final static Logger LOG = LoggerFactory.getLogger(ConsumerKafkaTopology.class);

	public static void main(String[] args) throws InterruptedException {
		final TridentTopology trident = new TridentTopology();
		KafkaSpoutConfig<String, String> build = KafkaSpoutConfig.builder("node1:9092,node2:9092,node3:9092"
				, "storm")
				.setProp("auto.offset.reset", "earliest")
				.setProp("group.id", "g1")
				.setProp("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.setProp("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.build();
		trident.newStream("testStream2", new KafkaTridentSpoutOpaque<>(build))
				.map(new MyMapFunction(),new Fields("areaid","head"))
				.global()
				.groupBy(new Fields("areaid"))
				.aggregate(new Fields("head"),new HeadSumReducer(),new Fields("headTimes"))
				.parallelismHint(3)
				.peek(input -> LOG.warn("{}-{}", input.getFields(), input));

		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(2);
		runThenStop("testTopology3", conf, trident.build(), 60);
	}
	private static class MyMapFunction implements MapFunction {
		private static final Logger LOG= LoggerFactory.getLogger(ConsumerKafkaTopology.MyMapFunction.class);
		@Override
		public Values execute(TridentTuple tridentTuple) {
			String[] roiValues=tridentTuple.getStringByField("value").split(",");
			return new Values(roiValues[0],Integer.parseInt(roiValues[1]));
		}
	}

	private static class HeadSumReducer implements ReducerAggregator<Integer> {
		@Override
		public Integer init() {
			return 0;
		}

		@Override
		public Integer reduce(Integer integer, TridentTuple tridentTuple) {
			return integer+tridentTuple.getIntegerByField("head");
		}
	}
}

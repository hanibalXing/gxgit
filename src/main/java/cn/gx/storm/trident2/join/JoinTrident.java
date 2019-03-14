package cn.gx.storm.trident2.join;

import org.apache.storm.Config;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static cn.gx.storm.utils.Runner.runThenStop;
public class JoinTrident {
	private final static Logger LOG = LoggerFactory.getLogger(JoinTrident.class);

	public static void main(String[] args) throws InterruptedException {
		FixedBatchSpout spout1 = new FixedBatchSpout(new Fields("a", "b"), 3,
				new Values(1, 4),
				new Values(1, 1),
				new Values(24, 5));
		spout1.setCycle(false);

		FixedBatchSpout spout2 = new FixedBatchSpout(new Fields("x", "c", "b"), 3,
				new Values(14, "s21", 40),
				new Values(14, "s22", 11),
				new Values(24, "s23", 24),
				new Values(24, "s24", 54));
		spout2.setCycle(false);

		final Config conf = new Config();
		conf.setNumWorkers(4);
		conf.setDebug(false);

		TridentTopology topology = new TridentTopology();
		Stream s1 = topology.newStream("test1", spout1).parallelismHint(1);
		//.peek(input -> LOG.info("1 {}-{}", input.getFields(), input));
		Stream s2 = topology.newStream("test2", spout2).parallelismHint(1);
		//.peek(input -> LOG.info("2 {}-{}", input.getFields(), input));
		//仅限于同一个batch
		topology.join(s1, new Fields("a"), s2, new Fields("x"), new Fields("d", "e", "f", "g"))
				.peek(input -> LOG.info("3 {}-{}", input.getFields(), input));

		runThenStop("mergeTrident", conf, topology.build(), 30);
	}

}

package cn.gx.storm.grouping.shuffle;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author gx
 * @ClassName: ShuffleGroupingTopology
 * @Description: java类作用描述
 * @date 2019/1/15 17:36
 * @Version: 1.0
 * @since
 */
public class ShuffleGroupingTopology {
	private final static Logger logger = LoggerFactory.getLogger(ShuffleGroupingTopology.class);
	/**
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		final TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout("StringGeneratorSpout", new ShuffleGroupingSpout()
				, 1);
		builder.setBolt("ShuffleGroupingBolt", new ShuffleGroupingBolt()
				, 2)
				.shuffleGrouping("StringGeneratorSpout");
		builder.setBolt("FiledGroupingFinalBolt", new ShuffleGroupingFinalBolt()
				, 2)
				.shuffleGrouping("ShuffleGroupingBolt");
		Config conf=new Config();
		conf.setNumWorkers(5);
		try {
			StormSubmitter.submitTopology("ShuffleGroupingTopology", conf,builder.createTopology());
			logger.warn("=============topology start=============");
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}

	}
	private static class Options{

		private final String topologyName;
		private final String prefix;
		private final int workers;
		private final int spoutParalleHint;
		private final int spoutTasks;
		private final int boltParalleHint;
		private final int blotTasks;

		public Options(String topologyName, String prefix, int workers,
		               int spoutParalleHint, int spoutTasks,
		               int boltParalleHint, int blotTasks) {
			this.topologyName = topologyName;
			this.prefix = prefix;
			this.workers = workers;
			this.spoutParalleHint = spoutParalleHint;
			this.spoutTasks = spoutTasks;
			this.boltParalleHint = boltParalleHint;
			this.blotTasks = blotTasks;
		}

		static Options bulid(String[] args) {
			return new Options(args[0] ,args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3])
					, Integer.parseInt(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]));
		}



		public String getTopologyName() {
			return topologyName;
		}

		public String getPrefix() {
			return prefix;
		}

		public int getWorkers() {
			return workers;
		}

		public int getSpoutParalleHint() {
			return spoutParalleHint;
		}

		public int getSpoutTasks() {
			return spoutTasks;
		}

		public int getBoltParalleHint() {
			return boltParalleHint;
		}

		public int getBlotTasks() {
			return blotTasks;
		}
		@Override
		public String toString() {
			return "options{" +
					"topologyName='" + topologyName + '\'' +
					", prefix='" + prefix + '\'' +
					", workers=" + workers +
					", spoutParalleHint=" + spoutParalleHint +
					", spoutTasks=" + spoutTasks +
					", boltParalleHint=" + boltParalleHint +
					", blotTasks=" + blotTasks +
					'}';
		}
	}
}

package cn.gx.storm.transaction;

import cn.gx.storm.wordcount.WordLineSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: NoTransactionTopology
 * @Description: java类作用描述
 * @date 2019/2/20 14:20
 * @Version: 1.0
 * @since
 */
public class NoTransactionTopology {
	private static final Logger LOG= LoggerFactory.getLogger(NoTransactionTopology.class);

	public static void main(String[] args) throws InterruptedException {
		TridentTopology tridentTopology=new TridentTopology();
		tridentTopology.newStream("no-transaction",new NoTransactionalBathchSpout(5,Arrays.asList(
						new Values("1"),new Values("1"),
						new Values("3"),new Values("3"),
						new Values("5"),new Values("6"),
						new Values("7"),new Values("8"),
						new Values("9"),new Values("10"),
						new Values("11"),new Values("12")
						),
				new Fields("name")))
				.parallelismHint(1)
				.groupBy(new Fields("name"))
				.aggregate(new Count(),new Fields("count"))
				.parallelismHint(5)
				.peek(tridentTuple -> LOG.info("{}",tridentTuple));
		final Config config = new Config();
		config.setNumWorkers(2);
		config.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("NoTransactionTopology", config, tridentTopology.build());
		TimeUnit.SECONDS.sleep(15);
		cluster.killTopology("NoTransactionTopology");
		cluster.shutdown();


	}

	public static class NoTransactionalBathchSpout implements IBatchSpout{
		private static final Logger LOG= LoggerFactory.getLogger(NoTransactionalBathchSpout.class);
		private final int batchSize;
		private final List<Values> values;
		private final Fields fields;
		private int index;
		private Map<Long,List<Values>> batches;

		public NoTransactionalBathchSpout(int batchSize, List<Values> values, Fields fields) {
			this.batchSize = batchSize;
			this.values = values;
			this.fields = fields;
		}

		@Override
		public void open(Map map, TopologyContext topologyContext) {
			this.index=0;
			this.batches=new HashMap<>(16);

		}

		@Override
		public void emitBatch(long l, TridentCollector tridentCollector) {
			final List<Values> batch=new ArrayList<>();
			for(int i=0;index<values.size()&&i<batchSize;i++,index++){
				batch.add(values.get(index));
			}
			if(!batch.isEmpty()) {
				batches.put(l,batch);
				batch.forEach(tridentCollector::emit);
			}

		}

		@Override
		public void ack(long l) {
			this.batches.remove(l);
			LOG.info("batch id {} process successful ",l);
		}

		@Override
		public void close() {

		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			final Config config=new Config();
			config.setMaxTaskParallelism(1);
			return config;
		}

		@Override
		public Fields getOutputFields() {
			return fields;
		}
	}
}

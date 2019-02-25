package cn.gx.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.com.google.common.base.Splitter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author gx
 * @ClassName: ManualDRPCTopology
 * @Description: java类作用描述
 * @date 2019/2/19 18:11
 * @Version: 1.0
 * @since
 */
public class ManualDRPCTopology {
	private static final Logger LOG=LoggerFactory.getLogger(ManualDRPCTopology.class);

	public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
		TopologyBuilder builder=new TopologyBuilder();
		DRPCSpout spout=new DRPCSpout("add");
		builder.setSpout("drpc",spout);
		builder.setBolt("add",new AddBolt(),3).shuffleGrouping("drpc");
		builder.setBolt("return",new ReturnResults(),3).shuffleGrouping("add");
		Config conf=new Config();
		conf.setDebug(false);
		conf.setNumWorkers(2);
		StormSubmitter.submitTopology("ManualDRPCTopology",conf,builder.createTopology());
	}

	public static class AddBolt extends BaseBasicBolt{

		@Override
		public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
			String params = tuple.getString(0);
			Object value = tuple.getValue(1);
			LOG.info("params:================{}",params);
			LOG.info("value:================{}",value);
			int sum = Arrays.stream(params.split(",")).mapToInt(Integer::parseInt).sum();
			//int sum = Splitter.on(",").splitToList(params).stream().mapToInt(Integer::parseInt).sum();
			basicOutputCollector.emit(new Values(String.valueOf(sum),value));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
			outputFieldsDeclarer.declare(new Fields("result","return-info"));
		}
	}
}

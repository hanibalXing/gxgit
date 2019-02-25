package cn.gx.storm.grouping.fileds;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
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
public class FiledGroupingTopology {
	private final static Logger logger = LoggerFactory.getLogger(FiledGroupingTopology.class);
	/**
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		final TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout("StringGeneratorSpout", new StringGeneratorSpout()
				, 1);
	/*	builder.setBolt("FiledGroupingBolt", new FiledGroupingBolt()
				, 2)
				.shuffleGrouping("StringGeneratorSpout");*/
		builder.setBolt("FiledGroupingFinalBolt", new FiledGroupingFinalBolt()
				, 2)
				.fieldsGrouping("StringGeneratorSpout",new Fields("name"));
		Config conf=new Config();
		conf.setNumWorkers(2);
		try {
			StormSubmitter.submitTopology("FiledGroupingTopology", conf,builder.createTopology());
			logger.warn("=============topology start=============");
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}

	}
}

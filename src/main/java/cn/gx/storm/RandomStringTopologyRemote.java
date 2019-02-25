package cn.gx.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author gx
 * @ClassName: RandomStringTopologyRemote
 * @Description: java类作用描述
 * @date 2019/1/11 15:31
 * @Version: 1.0
 * @since
 */
public class RandomStringTopologyRemote  {
	public static void main(String[] args) {
		final TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout("RandomStringSpout",new RandomStringSpout());
		builder.setBolt("starBolt",new WrapStarBolt(), 4).shuffleGrouping("RandomStringSpout");
		builder.setBolt("wellBolt",new WrapWellBolt(), 4).shuffleGrouping("RandomStringSpout");
		Config conf=new Config();
		conf.setNumWorkers(3);
		try {
			StormSubmitter.submitTopology("RandomStringTopologyRemote",conf,builder.createTopology());
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			e.printStackTrace();
		}
	}
}

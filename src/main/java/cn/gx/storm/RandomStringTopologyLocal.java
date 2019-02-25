package cn.gx.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: RandomStringTopologyLocal
 * @Description: java类作用描述
 * @date 2019/1/11 15:07
 * @Version: 1.0
 * @since
 */
public class RandomStringTopologyLocal {
	public static void main(String[] args) throws InterruptedException {
		final TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout("RandomStringSpout",new RandomStringSpout());
		builder.setBolt("starBolt",new WrapStarBolt(), 4).shuffleGrouping("RandomStringSpout");
		builder.setBolt("wellBolt",new WrapWellBolt(), 4).shuffleGrouping("RandomStringSpout");
		Config conf=new Config();
		conf.setDebug(true);

		LocalCluster cluster=new LocalCluster();
		cluster.submitTopology("RandomStringTopologyLocal", conf, builder.createTopology());

		System.out.println("running");
		TimeUnit.SECONDS.sleep(30);
		cluster.killTopology("RandomStringTopologyLocal");
		cluster.shutdown();
	}
}

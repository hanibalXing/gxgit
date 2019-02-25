package cn.gx.storm.tick;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: TickTopologyLocal
 * @Description: java类作用描述
 * @date 2019/2/12 21:56
 * @Version: 1.0
 * @since
 */
public class TickTopologyLocal {
    public static void main(String[] args) throws InterruptedException {
        final TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("NumberSpout",new NumberGenerateSpout(),2);
        builder.setBolt("NumberBolt",new NumberBolt(), 3).localOrShuffleGrouping("NumberSpout");
        //builder.setBolt("CountBolt",new CountBolt(), 1).globalGrouping("SplitBolt");

        Config conf=new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        conf.setMessageTimeoutSecs(5);

        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("WordCountTopologyLocal", conf, builder.createTopology());

        TimeUnit.SECONDS.sleep(15);
        cluster.killTopology("WordCountTopologyLocal");
        cluster.shutdown();
    }
}

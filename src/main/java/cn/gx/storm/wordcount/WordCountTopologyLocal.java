package cn.gx.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: TelephoneTopologyLocal
 * @Description: java类作用描述
 * @date 2019/2/11 5:44
 * @Version: 1.0
 * @since
 */
public class WordCountTopologyLocal {
    public static void main(String[] args) throws InterruptedException {
        final TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("WordLineSpout",new WordLineSpout());
        builder.setBolt("SplitBolt",new SplitBolt(), 3).localOrShuffleGrouping("WordLineSpout");
        builder.setBolt("CountBolt",new CountBolt(), 1).globalGrouping("SplitBolt");
        //构建config对象
        Config conf=new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        conf.setMessageTimeoutSecs(5);

        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("WordCountTopologyLocal", conf, builder.createTopology());

        System.out.println("running");
        TimeUnit.SECONDS.sleep(15);
        cluster.killTopology("WordCountTopologyLocal");
        cluster.shutdown();
    }
}

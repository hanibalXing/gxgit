package cn.gx.storm.telephone;

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
public class TelephoneTopologyLocal {
    public static void main(String[] args) throws InterruptedException {
        final TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("TelephoneSpout",new TelephoneSpout());
        //builder.setBolt("callTimeBolt",new TelephoneCallTimeBolt(), 1).globalGrouping("TelephoneSpout");
        builder.setBolt("callStatBolt",new TelephoneCallStatBolt(), 1).globalGrouping("TelephoneSpout");

        Config conf=new Config();
        conf.setDebug(true);

        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("TelephoneTopologyLocal", conf, builder.createTopology());

        System.out.println("running");
        TimeUnit.SECONDS.sleep(20);
        cluster.killTopology("TelephoneTopologyLocal");
        cluster.shutdown();
    }
}

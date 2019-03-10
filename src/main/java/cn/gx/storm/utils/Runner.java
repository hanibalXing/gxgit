package cn.gx.storm.utils;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

import java.util.concurrent.TimeUnit;

public final class Runner
{
    public static void runThenStop(String topologyName, Config conf, StormTopology topology, int wait, TimeUnit timeUnit) throws InterruptedException
    {
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        timeUnit.sleep(wait);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }

    public static void runThenStop(String topologyName, Config conf, StormTopology topology, int wait) throws InterruptedException
    {
        runThenStop(topologyName, conf, topology, wait, TimeUnit.SECONDS);
    }
}

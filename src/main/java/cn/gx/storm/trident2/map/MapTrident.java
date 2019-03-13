package cn.gx.storm.trident2.map;

import org.apache.storm.Config;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cn.gx.storm.utils.Runner.runThenStop;


public class MapTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(MapTrident.class);

    public static void main(String[] args) throws InterruptedException
    {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b", "c", "d"), 3,
                new Values(1, 4, 7, 10),
                new Values(1, 1, 3, 11),
                new Values(2, 2, 7, 1),
                new Values(2, 5, 7, 2));
        spout.setCycle(false);

        final Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(false);

        TridentTopology topology = new TridentTopology();
        topology.newStream("function", spout).parallelismHint(1)
                .localOrShuffle()
                .peek(input -> LOG.info("1 {}-{}", input.getFields(), input))
                .map(new MyMapFunction(), new Fields("name", "level")).parallelismHint(2)
                .localOrShuffle()
                .peek(input -> LOG.info("2 {}-{}", input.getFields(), input));

        runThenStop("FunctionTrident", conf, topology.build(), 30);
    }

    private static class MyMapFunction implements MapFunction
    {

        @Override
        public Values execute(TridentTuple input)
        {
            return new Values("Name:" + input.getIntegerByField("d"), input.getInteger(1));
        }
    }
}

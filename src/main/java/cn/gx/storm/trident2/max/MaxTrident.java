package cn.gx.storm.trident2.max;

import org.apache.storm.Config;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static cn.gx.storm.utils.Runner.runThenStop;


public class MaxTrident
{

    private static final Logger LOG = LoggerFactory.getLogger(MaxTrident.class);

    public static void main(String[] args) throws InterruptedException
    {
        TridentTopology trident = new TridentTopology();

        trident.newStream("test", new NoTransactionalBatchSpout())
                .parallelismHint(1).localOrShuffle()
                .max(new MaxComparator())
                .peek(input -> LOG.warn("{}-{}", input.getFields(), input));
        final Config conf = new Config();
        conf.setMaxSpoutPending(2);
        conf.setDebug(false);
        conf.setNumWorkers(3);
        runThenStop("test", conf, trident.build(), 1, TimeUnit.MINUTES);
    }

    private static class MaxComparator implements Comparator<TridentTuple>, Serializable
    {
        @Override
        public int compare(TridentTuple o1, TridentTuple o2)
        {
            return o1.getInteger(1) - o2.getInteger(1);
        }
    }
}

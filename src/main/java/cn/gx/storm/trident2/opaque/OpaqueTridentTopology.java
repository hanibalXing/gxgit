package cn.gx.storm.trident2.opaque;

import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static cn.gx.storm.utils.Runner.runThenStop;


public class OpaqueTridentTopology
{
    private static final Logger LOG = LoggerFactory.getLogger(OpaqueTridentTopology.class);

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException
    {
        TridentTopology trident = new TridentTopology();
        trident.newStream("OpaqueTridentStream", new OpaqueTridentSpout())
                .parallelismHint(1)
                .localOrShuffle()
                .each(new Fields("number"), new DoubleFunction(), new Fields("doubleNumber"))
                .parallelismHint(5)
                .peek(input -> LOG.warn("{}-{}", input.getFields(), input));

        final Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(3);
        conf.setMaxSpoutPending(2);
        runThenStop("OpaqueTridentTopology", conf, trident.build(), 1, TimeUnit.MINUTES);
    }

    private static class DoubleFunction extends BaseFunction
    {
        private static boolean error = true;

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector)
        {
            Integer value = tuple.getInteger(0);
            if (value == 3 && error)
            {
                error = false;
                throw new FailedException();
            }
            collector.emit(new Values(value * 2));
        }
    }
}